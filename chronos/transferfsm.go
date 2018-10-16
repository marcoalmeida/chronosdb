package chronos

import (
	"errors"
	"math/rand"
	"time"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"go.uber.org/zap"
)

func (d *Chronos) fsmTransferCollectTargets() (map[string][]*coretypes.Key, error) {
	dbs, err := d.influxDB.ShowDBs()
	if err != nil {
		d.logger.Error(
			"Failed to list DBs",
			zap.Error(err),
		)
		return nil, err
	}

	// find all keys (db, measurement) that this node keeps which is also relevant for other nodes
	targets := map[string][]*coretypes.Key{}
	for _, db := range dbs {
		measurements, err := d.influxDB.ShowMeasurements(db)
		if err != nil {
			d.logger.Error(
				"Failed to list measurements",
				zap.Error(err),
			)
			return nil, err
		}

		for _, m := range measurements {
			key := coretypes.NewKey(db, m)
			nodes := d.cluster.GetNodesRanked(key.String())
			if len(nodes) < d.cfg.NumberOfReplicas {
				d.logger.Error(
					"Not enough nodes available",
					zap.Int("need", d.cfg.NumberOfReplicas),
					zap.Int("found", len(nodes)),
				)
				return nil, errors.New("not enough replicas available")
			}
			// get the first N nodes responsible for this key
			nodes = nodes[:d.cfg.NumberOfReplicas]
			for _, n := range nodes {
				// skip self
				if n != d.cfg.NodeID {
					targets[n] = append(targets[n], key)
				}
			}
		}
	}

	return targets, nil
}

// transfer all keys to a given node in some random order (to minimize the probability of having more
// than one node trying to transfer the same data to the same target)
func (d *Chronos) fsmTransferAllKeys(node string, keys []*coretypes.Key) {
	order := rand.Perm(len(keys))
	for _, i := range order {
		key := keys[i]

		keyExists, err := d.gossip.AskHasKey(node, key)
		if err != nil {
			d.logger.Error(
				"Failed to query node for key",
				zap.Error(err),
				zap.String("node", node),
				zap.String("key", key.String()),
			)
			// nothing else we can do here; maybe the next round will succeed?
			continue
		}
		if keyExists {
			// no point on transferring it
			d.logger.Debug(
				"Skipping existing key",
				zap.String("node", node),
				zap.String("key", key.String()),
			)
		} else {
			// transfer the key, move on to the next one
			d.logger.Debug(
				"Starting to transfer key",
				zap.String("node", node),
				zap.String("key", key.String()),
			)
			d.fsmTransferKey(node, key)
		}
	}
}

// read the local InfluxDB and generate hinted handoffs -- unlike a typical handoff though, the target node needs to
// know when the transfer has completed, and both source and target must be able to resume from restarts
func (d *Chronos) fsmTransferKey(node string, key *coretypes.Key) {
	batch := 0
	transferResultChan := make(chan fsmForwardWriteResult, 1)

	for {
		metrics, err := d.influxDB.QueryToLineProtocol(
			key.DB,
			key.Measurement,
			d.cfg.KeyTransferBatchSize,
			d.cfg.KeyTransferBatchSize*batch,
		)
		if err != nil {
			// not much we can do; just make sure not to increment the counter so that we repeat the batch and carry on
			d.logger.Error(
				"Failed to query metrics",
				zap.Error(err),
				zap.String("node", node),
				zap.String("key", key.String()),
				zap.Error(err),
			)
			continue
		}
		if len(metrics) == 0 {
			d.logger.Info(
				"Key transfer completed",
				zap.String("node", node),
				zap.String("key", key.String()),
			)
			err := d.gossip.TellHasKey(node, key)
			for err != nil {
				d.logger.Error(
					"Failed to notify node that transfer is completed",
					zap.String("node", node),
					zap.String("key", key.String()),
					zap.Error(err),
				)
				// wait before retrying
				time.Sleep(time.Duration(d.cfg.KeyTransferInterval) * time.Second)
				err = d.gossip.TellHasKey(node, key)
			}
			return
		}
		d.fsmForwardWrite(node, d.createKeyTransferURL(key), key, metrics, transferResultChan)
		result := <-transferResultChan
		if result.httpStatus >= 200 && result.httpStatus <= 299 {
			d.logger.Debug("Successfully transferred key batch",
				zap.String("key", key.String()),
				zap.Int("batch", batch),
			)
			// move on to the next batch
			batch++
		} else {
			d.logger.Error("Failed to transfer key batch",
				zap.String("key", key.String()),
				zap.Int("batch", batch),
			)
		}
	}
}

package dynamo

import (
	"errors"
	"math/rand"
	"time"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"go.uber.org/zap"
)

func (dyn *Dynamo) fsmTransferCollectTargets() (map[string][]*coretypes.Key, error) {
	dbs, err := dyn.influxDB.ShowDBs()
	if err != nil {
		dyn.logger.Error(
			"Failed to list DBs",
			zap.Error(err),
		)
		return nil, err
	}

	// find all keys (db, measurement) that this node keeps which is also relevant for other nodes
	targets := map[string][]*coretypes.Key{}
	for _, db := range dbs {
		measurements, err := dyn.influxDB.ShowMeasurements(db)
		if err != nil {
			dyn.logger.Error(
				"Failed to list measurements",
				zap.Error(err),
			)
			return nil, err
		}

		for _, m := range measurements {
			key := coretypes.NewKey(db, m)
			nodes := dyn.ring.GetNodesRanked(key.String())
			if len(nodes) < dyn.cfg.NumberOfReplicas {
				dyn.logger.Error(
					"Not enough nodes available",
					zap.Int("need", dyn.cfg.NumberOfReplicas),
					zap.Int("found", len(nodes)),
				)
				return nil, errors.New("not enough replicas available")
			}
			// get the first N nodes responsible for this key
			nodes = nodes[:dyn.cfg.NumberOfReplicas]
			for _, n := range nodes {
				// skip self
				if n != dyn.cfg.NodeID {
					targets[n] = append(targets[n], key)
				}
			}
		}
	}

	return targets, nil
}

// transfer all keys to a given node in some random order (to minimize the probability of having more
// than one node trying to transfer the same data to the same target)
func (dyn *Dynamo) fsmTransferAllKeys(node string, keys []*coretypes.Key) {
	order := rand.Perm(len(keys))
	for _, i := range order {
		key := keys[i]

		keyExists, err := dyn.askHasKey(node, key)
		if err != nil {
			dyn.logger.Error(
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
			dyn.logger.Debug(
				"Skipping existing key",
				zap.String("node", node),
				zap.String("key", key.String()),
			)
		} else {
			// transfer the key, move on to the next one
			dyn.logger.Debug(
				"Starting to transfer key",
				zap.String("node", node),
				zap.String("key", key.String()),
			)
			dyn.fsmTransferKey(node, key)
		}
	}
}

// read the local InfluxDB and generate hinted handoffs -- unlike a typical handoff though, the target node needs to
// know when the transfer has completed, and both source and target must be able to resume from restarts
func (dyn *Dynamo) fsmTransferKey(node string, key *coretypes.Key) {
	batch := 0
	transferResultChan := make(chan fsmForwardWriteResult, 1)

	for {
		metrics, err := dyn.influxDB.QueryToLineProtocol(
			key.DB,
			key.Measurement,
			dyn.cfg.KeyTransferBatchSize,
			dyn.cfg.KeyTransferBatchSize*batch,
		)
		if err != nil {
			// not much we can do; just make sure not to increment the counter so that we repeat the batch and carry on
			dyn.logger.Error(
				"Failed to query metrics",
				zap.Error(err),
				zap.String("node", node),
				zap.String("key", key.String()),
				zap.Error(err),
			)
			continue
		}
		if len(metrics) == 0 {
			dyn.logger.Info(
				"Key transfer completed",
				zap.String("node", node),
				zap.String("key", key.String()),
			)
			err := dyn.tellHasKey(node, key)
			for err != nil {
				dyn.logger.Error(
					"Failed to notify node that transfer is completed",
					zap.String("node", node),
					zap.String("key", key.String()),
					zap.Error(err),
				)
				// wait before retrying
				time.Sleep(time.Duration(dyn.cfg.KeyTransferInterval) * time.Second)
				err = dyn.tellHasKey(node, key)
			}
			return
		}
		dyn.fsmForwardWrite(node, dyn.createKeyTransferURL(key), key, metrics, transferResultChan)
		result := <-transferResultChan
		if result.httpStatus >= 200 && result.httpStatus <= 299 {
			dyn.logger.Debug("Successfully transferred key batch",
				zap.String("key", key.String()),
				zap.Int("batch", batch),
			)
			// move on to the next batch
			batch++
		} else {
			dyn.logger.Error("Failed to transfer key batch",
				zap.String("key", key.String()),
				zap.Int("batch", batch),
			)
		}
	}
}

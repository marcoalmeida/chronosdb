package chronos

import (
	"math/rand"
	"time"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"github.com/marcoalmeida/chronosdb/influxdb"
	"github.com/marcoalmeida/chronosdb/shared"
	"go.uber.org/zap"
)

// detect keys missing on peers and local unnecessary keys -- check on startup
// re-balance by transferring each key over and deleting the ones that no longer belong
//
// for each key k stored locally
//   find all replicas that should own k
//   for each replica that should have k
//     if it does not have it -> start transferring k
//   upon success of all transfers, if the current node does not own k, remove it

// running continuously allows to send missing key at any moment in time
// besides helping with potentially weird issues like nodes dying midway through a transfer (either source or
// destination) this is also useful for when we implement auto discovery and don't need to restart ChronosDB to add
// nodes

func (c *Chronos) crosscheck() {
	// block indefinitely while the node is initializing
	for c.initializing {
		c.logger.Debug("Delaying cross-check until node initialization is completed")
		time.Sleep(time.Second * 3)
	}

	// TODO: block this while receiving intent log entries -- there might be some data left to transfer for a key
	//  this node no longer owns but should still send to others; for that to happen it must receive it all

	c.logger.Info("Starting cross-check task")

	for {
		targets := c.crosscheckFindTargets()
		if targets == nil {
			c.logger.Error("Cannot proceed with cross-check")
		}

		for replica, keys := range targets {
			c.crosscheckSend(replica, keys)
		}

		time.Sleep(time.Duration(c.cfg.CrossCheckSendInterval) * time.Second)
		//
		//	keys, err := c.getKeys()
		//	if err != nil {
		//		continue
		//	}
		//
		//	for _, k := range keys {
		//		replicas, err := c.getReplicas(k)
		//		if err != nil {
		//			c.logger.Error(
		//				"Not enough replicas available",
		//				zap.Int("need", c.cfg.NumberOfReplicas),
		//			)
		//			continue
		//		}
		//
		//		for _, r := range replicas {
		//			// skip self
		//			if r != c.cfg.NodeID {
		//				//targets[n] = append(targets[n], key)
		//			}
		//		}
		//	}
	}
}

// find all replicas that are expected to have a copy of each key stored in this node
// return a map replica -> keys or nil in case of error
func (c *Chronos) crosscheckFindTargets() map[string][]*coretypes.Key {
	dbs, err := c.influxDB.ShowDBs()
	if err != nil {
		c.logger.Error(
			"Failed to list DBs",
			zap.Error(err),
		)
		return nil
	}

	targets := map[string][]*coretypes.Key{}
	for _, db := range dbs {
		measurements, err := c.influxDB.ShowMeasurements(db)
		if err != nil {
			c.logger.Error(
				"Failed to list measurements",
				zap.Error(err),
			)
			return nil
		}

		for _, m := range measurements {
			key := coretypes.NewKey(db, m)
			replicas := c.cluster.GetNodesRanked(key.String())
			if len(replicas) < c.cfg.NumberOfReplicas {
				c.logger.Error(
					"Not enough replicas available",
					zap.Int("need", c.cfg.NumberOfReplicas),
					zap.Int("found", len(replicas)),
				)
				return nil
			}
			// get the first N replicas responsible for this key
			replicas = replicas[:c.cfg.NumberOfReplicas]
			for _, r := range replicas {
				// skip self
				if r != c.cfg.NodeID {
					targets[r] = append(targets[r], key)
				}
			}
		}
	}

	return targets
}

// TODO: transfer keys in parallel to multiple nodes + use a rate limiter to control outgoing traffic
// transfer all keys to a given node in some random order (to minimize the probability of having more
// than one node trying to transfer the same data to the same target)
func (c *Chronos) crosscheckSend(replica string, keys []*coretypes.Key) {
	order := rand.Perm(len(keys))
	for _, i := range order {
		key := keys[i]

		keyExists, err := c.gossip.AskHasKey(replica, key)
		if err != nil {
			c.logger.Error(
				"Cross-check: failed to query replica for key",
				zap.Error(err),
				zap.String("replica", replica),
				zap.String("key", key.String()),
			)
			// nothing else we can do here; maybe the next round will succeed?
			continue
		}
		if keyExists {
			// no point on transferring it
			c.logger.Debug(
				"Cross-check: skipping existing key",
				zap.String("replica", replica),
				zap.String("key", key.String()),
			)
		} else {
			// transfer the key, move on to the next one
			c.logger.Debug(
				"Cross-check: sending key",
				zap.String("replica", replica),
				zap.String("key", key.String()),
			)
			c.crosscheckSendKey(replica, key)
		}
	}
}

// TODO: rate limiting
// read the local InfluxDB, obtain the stored metrics in line protocol format, and forward them to the intended replica
// signal the replica once all data has been successfully send
func (c *Chronos) crosscheckSendKey(replica string, key *coretypes.Key) {
	batch := 0

	for {
		metrics, err := c.influxDB.QueryToLineProtocol(
			key.DB,
			key.Measurement,
			c.cfg.CrossCheckBatchSize,
			c.cfg.CrossCheckBatchSize*batch,
		)
		if err != nil {
			// not much we can do; just make sure not to increment the counter so that we repeat the batch and carry on
			c.logger.Error(
				"Cross-check: failed to query local data",
				zap.String("replica", replica),
				zap.String("key", key.String()),
				zap.Error(err),
			)
			continue
		}
		if len(metrics) == 0 {
			c.logger.Info(
				"Cross-check: all data sent",
				zap.String("replica", replica),
				zap.String("key", key.String()),
			)
			err := c.gossip.TellHasKey(replica, key)
			// TODO: set a limit on the number of retry attempts
			for err != nil {
				c.logger.Error(
					"Cross-check: failed to notify replica that all data was transferred",
					zap.String("replica", replica),
					zap.String("key", key.String()),
					zap.Error(err),
				)
				// wait before retrying
				time.Sleep(time.Second)
				err = c.gossip.TellHasKey(replica, key)
			}
			// TODO: remove the key in case this node is not on the list of replicas anymore
			return
		}
		// TODO: forwarding writes is a common operation, there should be a single function
		uri := influxdb.GenerateWriteURI(key.DB)
		headers := c.generateCrosscheckHeaders(key)
		status, _ := shared.DoPost(
			uri,
			metrics,
			headers,
			c.httpClient,
			c.cfg.MaxRetries,
			c.logger, "chronos.crosscheckSendKey",
		)

		if status >= 200 && status <= 299 {
			c.logger.Debug("Cross-check: successfully sent batch",
				zap.String("key", key.String()),
				zap.Int("batch", batch),
			)
			// move on to the next batch
			batch++
		} else {
			c.logger.Error("Cross-check: failed to send batch",
				zap.String("key", key.String()),
				zap.Int("batch", batch),
				zap.Int("http_status", status),
			)
		}
	}
}

package chronos

import (
	"errors"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
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
// destination [1]) this is also useful for when we implement auto discovery and don't need to restart ChronosDB to add
// nodes
//
// [1] - if the source dies midway through sending some key,
// the target replica will eventually start replying with "key missing" when asked if it's there.
// if this process runs continuously then some other replica will eventually check for that and start sending the
// data

// directory under which to store cross-check related information
const (
	// main directory
	crosscheckDirectory string = "cross-check"
	// markers for keys being received
	crosscheckReceiving string = "recv"
)

func (c *Chronos) crosscheck() {
	// block indefinitely while the node is initializing
	for c.initializing {
		c.logger.Debug("Delaying cross-check until node initialization is completed")
		time.Sleep(time.Second * 3)
	}

	// block the cross-check while receiving intent log entries
	//  there might be some data left to transfer for a key
	//  this node no longer owns but should still send to others;
	//  for that to happen it must receive it all
	if c.isRecovering(nil) {
		c.logger.Debug("Delaying cross-check until ")
		time.Sleep(time.Second * 3)
	}

	c.logger.Info("Starting cross-check task")

	// TODO: we need a setting to control how many replicas to send data to at the same time and how many bytes per
	//  second to transfer (basic rate limiting)
	// TODO: replicas returned from crosscheckFindTargets()
	for {
		targets := c.crosscheckFindTargets()
		if targets == nil {
			c.logger.Error("Cannot proceed with cross-check")
		} else {
			// TODO: transfer keys in parallel to multiple nodes + use a rate limiter to control outgoing traffic
			for replica, keys := range targets {
				c.crosscheckSend(replica, keys)
			}
		}

		time.Sleep(time.Duration(c.cfg.CrossCheckSendInterval) * time.Second)
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

// send all keys to a given node in some random order (to minimize the probability of having more
// than one node trying to send the same data to the same target)
func (c *Chronos) crosscheckSend(replica string, keys []*coretypes.Key) {
	// random order to use when iterating through the list of keys
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

// TODO: rate limiting (MB/s)
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
		// send the batch of data
		c.logger.Debug("Cross-check: sending batch",
			zap.String("key", key.String()),
			zap.Int("batch", batch),
		)
		uri := influxdb.GenerateWriteURI(key.DB)
		headers := http.Header{}
		c.request.SetCrosscheckHeaders(key, &headers)
		status, _ := c.forwardRequest(replica, &headers, uri, key, metrics)

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

func (c *Chronos) crosscheckMarker(key *coretypes.Key) string {
	return filepath.Join(c.cfg.DataDirectory, crosscheckDirectory, crosscheckReceiving, key.String())
}

// persist the key to disk to indicate it's being transferred
func (c *Chronos) crosscheckReceiving(key *coretypes.Key) error {
	marker := c.crosscheckMarker(key)
	c.logger.Debug(
		"Creating cross-check marker directory",
		zap.String("key", key.String()),
		zap.String("marker", marker),
	)

	err := shared.EnsureDirectory(marker)
	if err != nil {
		c.logger.Error(
			"Failed to create cross-check key marker",
			zap.String("key", key.String()),
			zap.Error(err),
		)

		return errors.New("failed to acknowledge cross-check")
	}

	return nil
}

// remove the marker that indicates that a key is being transferred
func (c *Chronos) crosscheckCompleted(key *coretypes.Key) error {
	marker := c.crosscheckMarker(key)
	c.logger.Debug(
		"Removing cross-check marker directory",
		zap.String("key", key.String()),
		zap.String("marker", marker),
	)

	err := os.Remove(marker)
	if err != nil {
		c.logger.Error(
			"Failed to remove cross-check key marker",
			zap.String("key", key.String()),
			zap.Error(err),
		)

		return errors.New("failed to complete cross-check")
	}

	return nil
}

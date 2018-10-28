package chronos

import (
	"net/http"
	"net/url"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"github.com/marcoalmeida/chronosdb/ilog"
	"github.com/marcoalmeida/chronosdb/influxdb"
	"github.com/marcoalmeida/chronosdb/shared"
	"go.uber.org/zap"
)

type fsmWriteResult struct {
	node string
	key  *coretypes.Key
	//db          string
	//measurement string
	httpStatus int
	response   []byte
}

type fsmForwardWriteResult struct {
	// target node
	node string
	// propagate the httpResponse body and status code from InfluxDB
	httpStatus int
	response   []byte
}

func (c *Chronos) fsmStartWrite(headers http.Header, uri string, form url.Values, payload []byte) (int, []byte) {
	// we need the DB name for a number of things, might as well extract it now and pass it along to avoid repeating
	// the same call
	db := influxdb.DBNameFromURL(form)

	if c.nodeIsCoordinator(headers) {
		c.logger.Debug("Coordinating write", zap.String("db", db), zap.String("node", c.cfg.NodeID))
		// start one coordinating task per measurement
		resultsChan := make(chan fsmWriteResult)
		metricsByMeasurement := influxdb.SplitMeasurements(payload)

		for measurement, metrics := range metricsByMeasurement {
			// create the partitioning key from the DB and measurement names
			key := coretypes.NewKey(db, measurement)
			go c.fsmCoordinateWrite(uri, key, metrics, resultsChan)
		}

		// wait for the results of all FSMs; return an error if any fails
		var status int
		var response []byte
		for i := 0; i < len(metricsByMeasurement); i++ {
			r := <-resultsChan
			// save for using outside of the loop
			status = r.httpStatus
			response = r.response
			if !(r.httpStatus >= 200 && r.httpStatus <= 299) {
				c.logger.Error(
					"Failed coordinated write",
					zap.String("key", r.key.String()),
					zap.String("node", r.node),
				)
				// return an error on any failures -- the client may want to retry
				return r.httpStatus, r.response
			} else {
				c.logger.Debug(
					"Successful coordinated write",
					zap.String("key", r.key.String()),
					zap.String("node", r.node),
				)
			}
		}
		// if we made it this far, all writes succeeded and any (status, httpResponse) pair is good to return
		return status, response
	} else {
		// write locally
		c.logger.Debug("Not coordinating: writing locally",
			zap.String("db", db),
			zap.String("node", c.cfg.NodeID),
		)
		return c.fsmWriteLocally(uri, db, payload)
	}
}

func (c *Chronos) fsmWriteLocally(origURI string, db string, metrics []byte) (int, []byte) {
	return c.influxDB.Write(origURI, db, metrics)
}

func (c *Chronos) fsmCoordinateWrite(
	uri string,
	key *coretypes.Key,
	metrics []byte,
	resultsChan chan<- fsmWriteResult,
) {
	// get the nodes to which this key should be written to
	nodes := c.cluster.GetNodesRanked(key.String())
	if len(nodes) < c.cfg.NumberOfReplicas {
		c.logger.Error(
			"Not enough nodes",
			zap.Int("need", c.cfg.NumberOfReplicas),
			zap.Int("found", len(nodes)))
		return
	}

	// select the top N replicas for this key
	nodes = nodes[:c.cfg.NumberOfReplicas]
	c.logger.Debug(
		"Writing metrics",
		zap.String("key", key.String()),
		zap.Strings("nodes", nodes),
	)

	// write to all nodes in parallel
	// TODO: optimization: if self is one of the nodes we should write to, write locally now and avoid forwarding the
	// TODO: data we already have
	forwardWriteResultsChan := make(chan fsmForwardWriteResult)
	for _, node := range nodes {
		go c.fsmForwardWrite(node, uri, key, metrics, forwardWriteResultsChan)
	}

	// if we got to write to enough nodes, save local intentLog for the other ones; otherwise signal failure
	c.fsmCheckWriteQuorum(nodes, uri, key, metrics, forwardWriteResultsChan, resultsChan)
}

func (c *Chronos) fsmForwardWrite(
	node string,
	origURI string,
	key *coretypes.Key,
	metrics []byte,
	forwardWriteResultsChan chan<- fsmForwardWriteResult,
) {
	c.logger.Debug("Forwarding write",
		zap.String("key", key.String()),
		zap.String("coordinator", c.cfg.NodeID),
		zap.String("target", node),
	)
	u := c.generateForwardURL(node, origURI)
	status, response := shared.DoPost(
		u,
		metrics,
		nil,
		c.httpClient,
		c.cfg.MaxRetries,
		c.logger, "chronos.fsmForwardWrite",
	)

	forwardWriteResultsChan <- fsmForwardWriteResult{node: node, httpStatus: status, response: response}
}

func (c *Chronos) fsmCheckWriteQuorum(
	nodes []string,
	uri string,
	key *coretypes.Key,
	metrics []byte,
	forwardWriteResultsChan <-chan fsmForwardWriteResult,
	resultsChan chan<- fsmWriteResult,
) {
	// make sure we wait for all nodes we forwarded the request to to finish
	// accumulate failures alone because that's all we need to act on -- save intentLog for later replay
	failures := []fsmForwardWriteResult{}
	// save for using outside of the loop
	var statusOK, statusFail int
	var responseOK, responseFail []byte
	for i := 0; i < c.cfg.NumberOfReplicas; i++ {
		result := <-forwardWriteResultsChan
		if result.httpStatus >= 400 && result.httpStatus <= 499 {
			// client side error, no point on trying to continue
			c.logger.Error(
				"Write failed: client-side error",
				zap.String("node", result.node),
				zap.String("key", key.String()),
				zap.Int("status", result.httpStatus),
				zap.ByteString("httpResponse", result.response),
			)
			resultsChan <- fsmWriteResult{
				key:        key,
				httpStatus: result.httpStatus,
				response:   result.response,
			}
			return
		}
		if !(result.httpStatus >= 200 && result.httpStatus <= 299) {
			failures = append(failures, result)
			statusFail = result.httpStatus
			responseFail = result.response
		} else {
			statusOK = result.httpStatus
			responseOK = result.response
		}
	}

	// great success, nothing else to do here
	if len(failures) == 0 {
		c.logger.Debug("Successfully wrote to all nodes",
			zap.Strings("nodes", nodes),
			zap.String("key", key.String()),
		)
		// all nodes reported success, any of the collected (statusOK, responseOK) pairs is good
		resultsChan <- fsmWriteResult{key: key, httpStatus: statusOK, response: responseOK}
		return
	}

	successfulWrites := c.cfg.NumberOfReplicas - len(failures)
	// we still succeeded if we wrote to enough nodes; just keep a local hint to replay later
	if successfulWrites > 0 && successfulWrites >= c.cfg.WriteQuorum {
		c.logger.Debug(
			"Write quorum met; storing local intentLog",
			zap.Int("write_quorum", c.cfg.WriteQuorum),
			zap.Int("successful_writes", successfulWrites),
		)
		// store local intentLog to replay later
		for _, fail := range failures {
			c.logger.Debug(
				"Writing hint",
				zap.String("node", fail.node),
				zap.String("key", key.String()),
			)
			err := c.fsmStoreHint(fail.node, uri, key, metrics)
			if err != nil {
				// if we can't store the hint the write effectively failed
				c.logger.Error("Failed to write hint", zap.Error(err), zap.String("node", fail.node))
				resultsChan <- fsmWriteResult{
					key:        key,
					httpStatus: fail.httpStatus,
					response:   fail.response,
				}
				return
			}
		}

		// all intentLog were successfully saved
		resultsChan <- fsmWriteResult{key: key, httpStatus: statusOK, response: responseOK}
	} else {
		// quorum not met
		c.logger.Error(
			"Write quorum not met or all nodes failed",
			zap.Int("write_quorum", c.cfg.WriteQuorum),
			zap.Int("successful_writes", successfulWrites))
		resultsChan <- fsmWriteResult{key: key, httpStatus: statusFail, response: responseFail}
	}
}

// write the payload to the local file system as a hint to be replayed later
func (c *Chronos) fsmStoreHint(
	node string,
	uri string,
	key *coretypes.Key,
	payload []byte,
) error {
	return c.intentLog.Add(&ilog.Entry{
		Node: node,
		URI:  c.createHandoffURL(uri, key),
		// TODO: include headers
		Key:     key,
		Payload: payload,
	})
}

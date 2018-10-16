package chronos

import (
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

func (d *Chronos) fsmStartWrite(uri string, form url.Values, payload []byte) (int, []byte) {
	// we need the DB name for a number of things, might as well extract it now and pass it along to avoid repeating
	// the same call
	db := influxdb.DBNameFromURL(form)

	if d.nodeIsCoordinator(form) {
		d.logger.Debug("Coordinating write", zap.String("db", db), zap.String("node", d.cfg.NodeID))
		// start one coordinating task per measurement
		resultsChan := make(chan fsmWriteResult)
		metricsByMeasurement := influxdb.SplitMeasurements(payload)

		for measurement, metrics := range metricsByMeasurement {
			// create the partitioning key from the DB and measurement names
			key := coretypes.NewKey(db, measurement)
			go d.fsmCoordinateWrite(uri, key, metrics, resultsChan)
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
				d.logger.Error(
					"Failed coordinated write",
					zap.String("key", r.key.String()),
					zap.String("node", r.node),
				)
				// return an error on any failures -- the client may want to retry
				return r.httpStatus, r.response
			} else {
				d.logger.Debug(
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
		d.logger.Debug("Not coordinating: writing locally",
			zap.String("db", db),
			zap.String("node", d.cfg.NodeID),
		)
		return d.fsmWriteLocally(uri, db, payload)
	}
}

func (d *Chronos) fsmWriteLocally(origURI string, db string, metrics []byte) (int, []byte) {
	return d.influxDB.Write(origURI, db, metrics)
}

func (d *Chronos) fsmCoordinateWrite(
	uri string,
	key *coretypes.Key,
	metrics []byte,
	resultsChan chan<- fsmWriteResult,
) {
	// get the nodes to which this key should be written to
	nodes := d.cluster.GetNodesRanked(key.String())
	if len(nodes) < d.cfg.NumberOfReplicas {
		d.logger.Error(
			"Not enough nodes",
			zap.Int("need", d.cfg.NumberOfReplicas),
			zap.Int("found", len(nodes)))
		return
	}

	// select the top N replicas for this key
	nodes = nodes[:d.cfg.NumberOfReplicas]
	d.logger.Debug(
		"Writing metrics",
		zap.String("key", key.String()),
		zap.Strings("nodes", nodes),
	)

	// write to all nodes in parallel
	// TODO: optimization: if self is one of the nodes we should write to, write locally now and avoid forwarding the
	// TODO: data we already have
	forwardWriteResultsChan := make(chan fsmForwardWriteResult)
	for _, node := range nodes {
		go d.fsmForwardWrite(node, uri, key, metrics, forwardWriteResultsChan)
	}

	// if we got to write to enough nodes, save local intentLog for the other ones; otherwise signal failure
	d.fsmCheckWriteQuorum(nodes, uri, key, metrics, forwardWriteResultsChan, resultsChan)
}

func (d *Chronos) fsmForwardWrite(
	node string,
	origURI string,
	key *coretypes.Key,
	metrics []byte,
	forwardWriteResultsChan chan<- fsmForwardWriteResult,
) {
	d.logger.Debug("Forwarding write",
		zap.String("key", key.String()),
		zap.String("coordinator", d.cfg.NodeID),
		zap.String("target", node),
	)
	u := d.createForwardURL(node, origURI)
	status, response := shared.DoPost(
		u,
		metrics,
		nil,
		d.httpClient,
		d.cfg.MaxRetries,
		d.logger, "chronos.fsmForwardWrite",
	)

	forwardWriteResultsChan <- fsmForwardWriteResult{node: node, httpStatus: status, response: response}
}

func (d *Chronos) fsmCheckWriteQuorum(
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
	for i := 0; i < d.cfg.NumberOfReplicas; i++ {
		result := <-forwardWriteResultsChan
		if result.httpStatus >= 400 && result.httpStatus <= 499 {
			// client side error, no point on trying to continue
			d.logger.Error(
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
		d.logger.Debug("Successfully wrote to all nodes",
			zap.Strings("nodes", nodes),
			zap.String("key", key.String()),
		)
		// all nodes reported success, any of the collected (statusOK, responseOK) pairs is good
		resultsChan <- fsmWriteResult{key: key, httpStatus: statusOK, response: responseOK}
		return
	}

	successfulWrites := d.cfg.NumberOfReplicas - len(failures)
	// we still succeeded if we wrote to enough nodes; just keep a local hint to replay later
	if successfulWrites > 0 && successfulWrites >= d.cfg.WriteQuorum {
		d.logger.Debug(
			"Write quorum met; storing local intentLog",
			zap.Int("write_quorum", d.cfg.WriteQuorum),
			zap.Int("successful_writes", successfulWrites),
		)
		// store local intentLog to replay later
		for _, fail := range failures {
			d.logger.Debug(
				"Writing hint",
				zap.String("node", fail.node),
				zap.String("key", key.String()),
			)
			err := d.fsmStoreHint(fail.node, uri, key, metrics)
			if err != nil {
				// if we can't store the hint the write effectively failed
				d.logger.Error("Failed to write hint", zap.Error(err), zap.String("node", fail.node))
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
		d.logger.Error(
			"Write quorum not met or all nodes failed",
			zap.Int("write_quorum", d.cfg.WriteQuorum),
			zap.Int("successful_writes", successfulWrites))
		resultsChan <- fsmWriteResult{key: key, httpStatus: statusFail, response: responseFail}
	}
}

// write the payload to the local file system as a hint to be replayed later
func (d *Chronos) fsmStoreHint(
	node string,
	uri string,
	key *coretypes.Key,
	payload []byte,
) error {
	return d.intentLog.Add(&ilog.Entry{
		Node:    node,
		URI:     d.createHandoffURL(uri, key),
		Key:     key,
		Payload: payload,
	})
}

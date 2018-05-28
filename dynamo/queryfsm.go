package dynamo

import (
	"math/rand"
	"net/http"
	"net/url"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"github.com/marcoalmeida/chronosdb/influxdb"
	"github.com/marcoalmeida/chronosdb/shared"
	"go.uber.org/zap"
)

type fsmQueryResult struct {
	db          string
	measurement string
	httpStatus  int
	response    []byte
}

func (dyn *Dynamo) fsmStartQuery(
	uri string,
	form url.Values,
) (int, []byte) {
	if dyn.nodeIsCoordinator(form) {
		db := influxdb.DBNameFromURL(form)
		// each request may contain more than 1 query
		// break it down, run in parallel, and merge the results
		queries := influxdb.QueriesFromURL(form)
		resultsChan := make(chan fsmQueryResult)
		dyn.logger.Debug("Coordinating query", zap.String("db", db), zap.String("node", dyn.cfg.NodeID))
		// one task per query
		for _, q := range queries {
			go dyn.fsmCoordinateQuery(uri, form, q, resultsChan)
		}

		// wait for the results of all tasks and merge the results
		var status int
		var response []byte
		var allResponses [][]byte = make([][]byte, len(queries))
		for i := 0; i < len(queries); i++ {
			r := <-resultsChan
			status = r.httpStatus
			response = r.response
			if !(r.httpStatus >= 200 && r.httpStatus <= 299) {
				dyn.logger.Error(
					"Failed query",
					zap.String("db", r.db),
					zap.String("measurement", r.measurement),
				)
				// return an error on any failed query -- the client needs to deal with it
				return r.httpStatus, r.response
			} else {
				dyn.logger.Debug(
					"Successful query",
					zap.String("db", r.db),
					zap.String("measurement", r.measurement),
				)
				allResponses[i] = response
			}
		}

		return dyn.fsmMergeResponses(status, allResponses, form.Get("pretty") == "true")
	} else {
		// run the query locally and return the results
		dyn.logger.Debug("Querying locally",
			zap.String("db", form.Get("db")),
			zap.String("node", dyn.cfg.NodeID),
		)

		return dyn.fsmQueryLocally(uri, form)
	}
}

func (dyn *Dynamo) fsmMergeResponses(status int, responses [][]byte, pretty bool) (int, []byte) {
	merged, err := dyn.influxDB.MergeResponses(responses, pretty)
	if err != nil {
		dyn.logger.Error("Failed to merge responses from queries", zap.Error(err))
		return http.StatusInternalServerError, nil
	}

	return status, merged
}

func (dyn *Dynamo) fsmQueryLocally(uri string, form url.Values) (int, []byte) {
	return dyn.influxDB.Query(uri, form)
}

func (dyn *Dynamo) fsmCoordinateQuery(
	uri string,
	form url.Values,
	query string,
	resultsChan chan<- fsmQueryResult,
) {
	// create the partitioning key from the DB and measurement names
	db := influxdb.DBNameFromURL(form)
	measurement := influxdb.MeasurementNameFromQuery(query)
	key := coretypes.NewKey(db, measurement)

	// get the nodes this key should use
	nodes := dyn.ring.GetNodesRanked(key.String())
	if len(nodes) < dyn.cfg.NumberOfReplicas {
		dyn.logger.Error(
			"Not enough nodes",
			zap.Int("need", dyn.cfg.NumberOfReplicas),
			zap.Int("found", len(nodes)))
		return
	}

	// select the top N replicas for this key
	nodes = nodes[:dyn.cfg.NumberOfReplicas]
	dyn.logger.Debug(
		"Querying data",
		zap.String("db", db),
		zap.String("measurement", measurement),
		zap.Strings("nodes", nodes),
	)

	// order by which the nodes will be queried (mostly irrelevant for writes, as we want to write to all nodes)
	// when querying though, we use a random ordering to try and spread the load evenly among all replicas
	ordering := rand.Perm(dyn.cfg.NumberOfReplicas)
	var status int
	var response []byte
	for _, i := range ordering {
		node := nodes[i]
		dyn.logger.Debug("Querying node", zap.String("node", node))
		status, response = dyn.fsmForwardQuery(node, uri, form)
		// just call fsmForwardQuery and return as soon as the first remote call returns successfully
		if status >= 200 && status <= 299 {
			resultsChan <- fsmQueryResult{
				db:          db,
				measurement: measurement,
				httpStatus:  status,
				response:    response,
			}
			return
		} else {
			dyn.logger.Debug(
				"Failed to query node",
				zap.String("node", node),
				zap.String("db", db),
				zap.String("measurement", measurement),
				zap.Int("status", status),
			)
		}
	}

	// if we made it this far, all requests failed; result goes in the channel
	resultsChan <- fsmQueryResult{
		db:          db,
		measurement: measurement,
		httpStatus:  status,
		response:    response,
	}
}

func (dyn *Dynamo) fsmForwardQuery(
	node string,
	uri string,
	form url.Values,
) (status int, response []byte) {
	dyn.logger.Debug("Forwarding query",
		zap.String("db", influxdb.DBNameFromURL(form)),
		zap.String("coordinator", dyn.cfg.NodeID),
		zap.String("target", node),
	)
	u := dyn.createForwardURL(node, uri)
	return shared.DoPost(
		u,
		[]byte(form.Encode()),
		nil,
		dyn.httpClient,
		dyn.cfg.MaxRetries,
		dyn.logger, "dynamo.fsmForwardQuery",
	)
}

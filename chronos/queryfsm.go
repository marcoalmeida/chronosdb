package chronos

import (
	"hash/fnv"
	"net/http"
	"net/url"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"github.com/marcoalmeida/chronosdb/influxdb"
	"github.com/marcoalmeida/chronosdb/shared"
	"go.uber.org/zap"
)

type fsmQueryResult struct {
	node         string
	httpStatus   int
	httpResponse []byte
}

func (d *Chronos) fsmStartQuery(
	uri string,
	form url.Values,
) (int, []byte) {
	// run the query locally and return the results
	if !d.nodeIsCoordinator(form) {
		d.logger.Debug("Querying locally",
			zap.String("db", form.Get("db")),
			zap.String("node", d.cfg.NodeID),
		)

		return d.fsmRunQuery(uri, form)
	}

	// act as a coordinator and distribute the query among all replicas that hold the key this query is operating on
	db := influxdb.DBNameFromURL(form)
	query := influxdb.QueryFromURL(form)
	d.logger.Debug(
		"Coordinating query",
		zap.String("db", db),
		zap.String("node", d.cfg.NodeID),
		zap.String("query", query),
	)

	return d.fsmCoordinateQuery(uri, form)
}

func (d *Chronos) fsmRunQuery(uri string, form url.Values) (int, []byte) {
	// both hinted hand offs and key transfers include the key name in the query string, so this is safe
	key := d.getKeyFromURL(form)
	// if the node is in recovering mode for the key being queried we can't proceed
	if d.isRecovering(key) {
		return http.StatusServiceUnavailable, []byte("in recovery")
	}

	return d.influxDB.Query(uri, form)
}

// send the query to all replicas that hold the key this query is operating on
func (d *Chronos) fsmCoordinateQuery(
	uri string,
	form url.Values,
) (int, []byte) {
	// create the partitioning key from the DB and measurement names
	db := influxdb.DBNameFromURL(form)
	query := influxdb.QueryFromURL(form)
	measurement := influxdb.MeasurementNameFromQuery(query)
	key := coretypes.NewKey(db, measurement)

	// get the replicas this key should use
	nodes := d.cluster.GetNodesRanked(key.String())
	if len(nodes) < d.cfg.NumberOfReplicas {
		d.logger.Error(
			"Not enough nodes",
			zap.Int("need", d.cfg.NumberOfReplicas),
			zap.Int("found", len(nodes)))
		return http.StatusInternalServerError, []byte("not enough nodes")
	}

	// select the top N replicas for this key
	nodes = nodes[:d.cfg.NumberOfReplicas]
	d.logger.Debug("Querying data", zap.String("key", key.String()), zap.Strings("nodes", nodes))
	// run the query on all replicas in parallel
	resultsChan := make(chan fsmQueryResult)
	for _, node := range nodes {
		d.logger.Debug("Querying node", zap.String("node", node))
		go d.fsmForwardQuery(node, uri, form, resultsChan)
	}

	// collect the results
	results := make([]fsmQueryResult, 0)
	for i := 0; i < len(nodes); i++ {
		r := <-resultsChan
		if r.httpStatus >= 200 && r.httpStatus <= 299 {
			results = append(results, r)
		} else {
			d.logger.Debug(
				"Failed to query node",
				zap.String("node", r.node),
				zap.String("db", key.DB),
				zap.String("measurement", key.Measurement),
				zap.Int("status", r.httpStatus),
			)
		}
	}

	// make sure we have quorum, i.e., enough total nodes and at least read_quorum equal responses
	if len(results) < d.cfg.ReadQuorum {
		return http.StatusInternalServerError, []byte("not enough nodes")
	}
	// use a hash to count the number of equal responses
	quorum := make(map[uint64]int, 0)
	for _, r := range results {
		h := fnv.New64a()
		h.Write(r.httpResponse)
		sum := h.Sum64()
		n, ok := quorum[sum]
		if ok {
			quorum[sum] = n + 1
			if quorum[sum] == d.cfg.ReadQuorum {
				return http.StatusOK, r.httpResponse
			}
		} else {
			quorum[sum] = 1
		}
	}

	// let the client decide how
	return http.StatusInternalServerError, []byte("no quorum")
}

func (d *Chronos) fsmForwardQuery(
	node string,
	uri string,
	form url.Values,
	resultsChan chan<- fsmQueryResult,
) {
	d.logger.Debug("Forwarding query",
		zap.String("db", influxdb.DBNameFromURL(form)),
		zap.String("coordinator", d.cfg.NodeID),
		zap.String("target", node),
	)
	u := d.createForwardURL(node, uri)
	status, response := shared.DoPost(
		u,
		[]byte(form.Encode()),
		nil,
		d.httpClient,
		d.cfg.MaxRetries,
		d.logger, "chronos.fsmForwardQuery",
	)

	resultsChan <- fsmQueryResult{
		node:         node,
		httpStatus:   status,
		httpResponse: response,
	}
}

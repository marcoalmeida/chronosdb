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

func (c *Chronos) fsmStartQuery(
	uri string,
	form url.Values,
) (int, []byte) {
	// run the query locally and return the results
	if !c.nodeIsCoordinator(form) {
		c.logger.Debug("Running query locally",
			zap.String("db", form.Get("db")),
			zap.String("node", c.cfg.NodeID),
		)

		return c.fsmRunQuery(uri, form)
	}

	// act as a coordinator and distribute the query among all replicas that hold the key this query is operating on
	return c.fsmCoordinateQuery(uri, form)
}

func (c *Chronos) fsmRunQuery(uri string, form url.Values) (int, []byte) {
	// the key is always added by the coordinator
	key := getKeyFromURL(form)
	if key == nil {
		// TODO: handle error
	}
	// if the node is in recovering mode for the key being queried we can't proceed
	if c.isRecovering(key) {
		return http.StatusServiceUnavailable, []byte("in recovery")
	}

	return c.influxDB.Query(uri, form)
}

// send the query to all replicas that hold the key this query is operating on
func (c *Chronos) fsmCoordinateQuery(
	uri string,
	form url.Values,
) (int, []byte) {
	// create the partitioning key from the DB and measurement names
	db := influxdb.DBNameFromURL(form)
	query := influxdb.QueryFromURL(form)
	measurement := influxdb.MeasurementNameFromQuery(query)
	key := coretypes.NewKey(db, measurement)

	c.logger.Debug(
		"Coordinating query",
		zap.String("db", db),
		zap.String("node", c.cfg.NodeID),
		zap.String("query", query),
	)

	// get the replicas this key should use
	nodes := c.cluster.GetNodesRanked(key.String())
	if len(nodes) < c.cfg.NumberOfReplicas {
		c.logger.Error(
			"Not enough nodes",
			zap.Int("need", c.cfg.NumberOfReplicas),
			zap.Int("found", len(nodes)))
		return http.StatusInternalServerError, []byte("not enough nodes")
	}

	// select the top N replicas for this key
	nodes = nodes[:c.cfg.NumberOfReplicas]
	c.logger.Debug("Querying data", zap.String("key", key.String()), zap.Strings("nodes", nodes))
	// run the query on all replicas in parallel
	resultsChan := make(chan fsmQueryResult)
	for _, node := range nodes {
		c.logger.Debug("Querying node", zap.String("node", node))
		// TODO: add key information
		go c.fsmForwardQuery(node, uri, form, resultsChan)
	}

	// collect the results
	results := make([]fsmQueryResult, 0)
	for i := 0; i < len(nodes); i++ {
		r := <-resultsChan
		if r.httpStatus >= 200 && r.httpStatus <= 299 {
			results = append(results, r)
		} else {
			c.logger.Debug(
				"Failed to query node",
				zap.String("node", r.node),
				zap.String("db", key.DB),
				zap.String("measurement", key.Measurement),
				zap.Int("status", r.httpStatus),
			)
		}
	}

	// TODO: this can be greatly improved (we also check for quorum on init)
	// make sure we have quorum, i.e., enough total nodes and at least read_quorum equal responses
	if len(results) < c.cfg.ReadQuorum {
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
			if quorum[sum] == c.cfg.ReadQuorum {
				return http.StatusOK, r.httpResponse
			}
		} else {
			quorum[sum] = 1
		}
	}

	// let the client decide how
	return http.StatusInternalServerError, []byte("no quorum")
}

func (c *Chronos) fsmForwardQuery(
	node string,
	uri string,
	form url.Values,
	resultsChan chan<- fsmQueryResult,
) {
	c.logger.Debug("Forwarding query",
		zap.String("db", influxdb.DBNameFromURL(form)),
		zap.String("coordinator", c.cfg.NodeID),
		zap.String("target", node),
	)
	u := c.createForwardURL(node, uri)
	status, response := shared.DoPost(
		u,
		[]byte(form.Encode()),
		nil,
		c.httpClient,
		c.cfg.MaxRetries,
		c.logger, "chronos.fsmForwardQuery",
	)

	resultsChan <- fsmQueryResult{
		node:         node,
		httpStatus:   status,
		httpResponse: response,
	}
}

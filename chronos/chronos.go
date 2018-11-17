package chronos

import (
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/marcoalmeida/chronosdb/config"
	"github.com/marcoalmeida/chronosdb/coretypes"
	"github.com/marcoalmeida/chronosdb/gossip"
	"github.com/marcoalmeida/chronosdb/ilog"
	"github.com/marcoalmeida/chronosdb/influxdb"
	"github.com/marcoalmeida/chronosdb/request"
	"github.com/marcoalmeida/chronosdb/responsetypes"
	"github.com/marcoalmeida/chronosdb/shared"
	"github.com/marcoalmeida/hrw"
	"go.uber.org/zap"
)

type Chronos struct {
	cfg        *config.ChronosCfg
	logger     *zap.Logger
	cluster    hrw.Nodes
	gossip     *gossip.Gossip
	request    *request.Request
	intentLog  *ilog.ILog
	httpClient *http.Client
	influxDB   *influxdb.InfluxDB
	// node is initializing
	initializing bool
	// last time a key was received (replayed from an intent log, bootstrapping, or re-balancing)
	recovering   map[*coretypes.Key]time.Time
	recoveryLock sync.RWMutex
}

func New(cfg *config.ChronosCfg, logger *zap.Logger) *Chronos {
	// add all nodes listed in the configuration file to the cluster
	cluster := hrw.New(nil)
	for node, weight := range cfg.Nodes {
		logger.Debug("Adding node to the cluster", zap.String("node", node), zap.Float64("weight", weight))
		err := cluster.AddNode(hrw.Node{Name: node, Weight: weight})
		if err != nil {
			logger.Fatal("Failed to add node to cluster", zap.Error(err), zap.String("node", node))
		}
	}

	// it's safe to share one single HTTP client instance
	httpClient := shared.NewHTTPClient(cfg.ConnectTimeout, cfg.ClientTimeout)

	return &Chronos{
		cfg:          cfg,
		cluster:      cluster,
		gossip:       gossip.New(cfg.Port, httpClient, cfg.MaxRetries, logger),
		request:      request.New(cfg.NodeID, cfg.Port, cfg.ConnectTimeout, cfg.ClientTimeout, cfg.MaxRetries, logger),
		intentLog:    ilog.New(cfg.DataDirectory, logger),
		initializing: true,
		recovering:   make(map[*coretypes.Key]time.Time, 0),
		logger:       logger,
		httpClient:   httpClient,
		influxDB:     influxdb.New(&cfg.InfluxDB, logger),
	}
}

func (c *Chronos) Start() {
	// cleanup intentLog: make sure we don't have any dangling entries (i.e., replay started but was interrupted)
	c.intentLog.RestoreDangling()
	// delete stale intent log entries (i.e., intended for nodes that are no longer part of the cluster)
	c.intentLog.RemoveStale(c.cluster.GetAllNodes())

	// all of the following need to be background tasks so that Start() can return immediately
	// without blocking the caller (which needs to listen-and-serve)

	// initialize the node -- create all DBs, users, ...
	go c.initialize()
	// continuously replay all entries in the intent log
	go c.replayIntentLog()
	// transfer keys that other replicas should also own but don't yet have (temporarily down, new node bootstrapping)
	// drop keys (after transferring them) that are no longer owned by this node (new nodes were added)
	go c.crosscheck()
	// check for the last write timestamp in recovery mode (for each key) and exit if past the grace period
	go c.checkAndExitRecovery()
}

// continuously query the Intent Log for available entries and try to replay them
func (c *Chronos) replayIntentLog() {
	c.logger.Info("Starting to replay the Intent Log")
	wait := c.cfg.ReplayInterval
	// forwardWriteResultsChan := make(chan fsmForwardWriteResult, 1)

	for {
		c.logger.Debug("Sleeping between intent log entries", zap.Int("time", wait))
		time.Sleep(time.Second * time.Duration(wait))
		entry, err := c.intentLog.Fetch()
		if err != nil {
			c.logger.Error("Failed to Fetch an entry from the intent log", zap.Error(err))
			continue
		}

		if entry == nil {
			// no log entries to process, extend the sleep time up to the maximum as set in the configuration file
			if wait == 0 {
				wait = 1
			} else {
				wait = shared.Min(wait*2, c.cfg.ReplayInterval)
			}
			continue
		} else {
			// there are more log entries to process, reduce the waiting period
			if wait > 0 {
				wait /= 2
			}
		}

		c.logger.Info(
			"Processing intent log entry",
			zap.String("node", entry.Node),
			zap.String("key", entry.Key.String()),
			zap.String("uri", entry.URI),
		)
		headers := &http.Header{}
		c.request.SetIntentLogHeaders(entry.Key, headers)
		status, _ := c.forwardRequest(entry.Node, headers, entry.URI, entry.Key, entry.Payload)
		if status >= 200 && status <= 299 {
			err := c.intentLog.Remove(entry)
			if err != nil {
				c.logger.Error("Failed to remove intent log entry", zap.Error(err))
			}
		} else {
			c.logger.Error("Failed to replay intent log entry",
				zap.String("node", entry.Node),
				zap.String("key", entry.Key.String()),
				zap.String("uri", entry.URI),
			)
			// put the entry back for replaying later
			c.intentLog.ReAdd(entry)
		}
	}
}

func (c *Chronos) NodeStatus() *responsetypes.NodeStatus {
	return &responsetypes.NodeStatus{
		Initializing: c.initializing,
		Recovering:   c.recovering,
	}
}

func (c *Chronos) GetCluster() *responsetypes.GetRing {
	return &responsetypes.GetRing{Nodes: c.cluster.GetAllNodes()}
}

func (c *Chronos) GetDBs() (*responsetypes.GetDBs, error) {
	// even in recovery mode should be safe to return the list of DBs as only metrics are replayed
	dbs, err := c.influxDB.ShowDBs()
	if err != nil {
		c.logger.Error("Failed to list DBs", zap.Error(err))
		return nil, err
	}

	return &responsetypes.GetDBs{Databases: dbs}, nil
}

// try to create (or drop) and DB on all nodes in the cluster; on failure return the node that failed to comply
func (c *Chronos) createOrDropDB(
	headers http.Header,
	uri string,
	form url.Values,
	db string,
	action string,
) (string, error) {
	var err error

	// regardless of whether or not being a coordinator, create the DB locally
	c.logger.Debug(
		"Creating/Dropping DB",
		zap.String("db", db),
		zap.String("node", c.cfg.NodeID),
		zap.String("action", action),
	)

	switch action {
	case "DROP":
		err = c.influxDB.DropDB(db)
	case "CREATE":
		err = c.influxDB.CreateDB(db)

	}
	if err != nil {
		c.logger.Error(
			"Failed to create/drop DB",
			zap.String("db", db),
			zap.String("action", action),
			zap.String("node", c.cfg.NodeID),
			zap.Error(err),
		)

		return c.cfg.NodeID, errors.New("influxDB failed")
	}

	// if acting as coordinator, i.e., this request was not forwarded, forward it to all nodes (but itself)
	if c.request.NodeIsCoordinator(headers) {
		var status int
		var response []byte

		for _, node := range c.cluster.GetAllNodes() {
			if node != c.cfg.NodeID {
				u := c.request.GenerateForwardURL(node, uri)
				c.request.SetForwardHeaders(nil, &headers)
				c.logger.Debug("Forwarding request", zap.String("url", u), zap.String("action", action))
				switch action {
				case "DROP":
					status, response = shared.DoDelete(
						u,
						nil,
						headers,
						c.httpClient,
						c.cfg.MaxRetries,
						c.logger,
						"chronos.DropDB",
					)
				case "CREATE":
					status, response = shared.DoPut(
						u,
						nil,
						headers,
						c.httpClient,
						c.cfg.MaxRetries,
						c.logger,
						"chronos.CreateDB",
					)
				}

				if !(status >= 200 && status <= 299) {
					c.logger.Error(
						"Failed to create/drop DB; rolling back",
						zap.String("db", db),
						zap.String("action", action),
						zap.String("node", c.cfg.NodeID),
						zap.ByteString("httpResponse", response),
						zap.Error(err),
					)
					// try to rollback
					if action == "CREATE" {
						node, err := c.DropDB(headers, uri, form, db)
						if err != nil {
							c.logger.Error("Failed to rollback CREATE DB", zap.String("node", node))
						}
					}

					return node, errors.New("influxDB failed")
				}
			}
		}
	}

	// if we made it this far, nothing failed
	return "", nil
}

func (c *Chronos) CreateDB(headers http.Header, uri string, form url.Values, db string) (string, error) {
	return c.createOrDropDB(headers, uri, form, db, "CREATE")
}

func (c *Chronos) DropDB(headers http.Header, uri string, form url.Values, db string) (string, error) {
	return c.createOrDropDB(headers, uri, form, db, "DROP")
}

func (c *Chronos) Write(headers http.Header, uri string, form url.Values, payload []byte) (int, []byte) {
	// no writing metrics while initializing
	if c.initializing {
		return http.StatusServiceUnavailable, []byte("initializing")
	}

	// if the request is either an entry from an intent log being replayed,
	// put the node in recovery mode for that key
	if c.request.RequestIsIntentLog(headers) {
		c.logger.Debug("Handling intent log write")
		key := c.request.GetKeyFromHeader(headers)
		c.setRecovering(key)
	}

	// if the request is coming in as part of the cross-check process persist the key to disk so that we can survive
	// a temporary failure during the transfer process
	//
	// problem: transfer begins, receiving node dies midway through or sender node fails before finishing sending
	// the whole key; the next time this node is queried it'll say the key exists even though it's incomplete
	//
	// instead persist to disk once it starts receiving it and delete it only after receiving an ack from the source
	// confirming it's all done;
	if c.request.RequestIsCrosscheck(headers) {
		c.logger.Debug("Handling cross-check write")
		// if receiving data during the cross-check process the node should also enter recovery mode
		// for the key being transferred
		key := c.request.GetKeyFromHeader(headers)
		c.setRecovering(key)
		// persist the key to disk
		err := c.crosscheckReceive(key)
		// we should not accept the write if we can't acknowledge the request to cross-check a key
		if err != nil {
			return http.StatusServiceUnavailable, []byte(err.Error())
		}
	}

	return c.fsmStartWrite(headers, uri, form, payload)
}

func (c *Chronos) Query(headers http.Header, uri string, form url.Values) (int, []byte) {
	// no querying metrics while initializing
	if c.initializing {
		return http.StatusServiceUnavailable, []byte("initializing")
	}

	// TODO: distinguish between a request we should just pass through and one that ChronosDB needs to act on
	//  a key won't even exist on most of them
	// TODO: if this node is coordinating there will be no key in the headers
	// if the node is in recovering mode for the key being queried we can't proceed
	key := c.request.GetKeyFromHeader(headers)
	// TODO: this does not seem to be a good test condition after the code revamp
	if key != nil {
		if c.isRecovering(key) {
			return http.StatusServiceUnavailable, []byte("in recovery")
		}
	}

	return c.fsmStartQuery(headers, uri, form)
}

// remove the marker that indicates a key transfer is in progress
func (c *Chronos) KeyRecvCompleted(key *coretypes.Key) error {
	return c.crosscheckComplete(key)
}

// GetKeyStatus returns a data structure describing the current status the key: absent, transferring, present.
func (c *Chronos) GetKeyStatus(key *coretypes.Key) (*responsetypes.KeyGet, error) {
	// if a transfer is already in progress return true so that another one does not start
	if c.crosscheckIsReceiving(key) {
		c.logger.Debug("Receiving key transfer", zap.String("key", key.String()))
		return &responsetypes.KeyGet{Status: responsetypes.KeyTransferring}, nil
	}

	dbs, err := c.influxDB.ShowDBs()
	if err != nil {
		c.logger.Error(
			"Failed to list DBs",
			zap.Error(err),
		)
		return nil, errors.New("failed to list databases")
	}

	for _, db := range dbs {
		measurements, err := c.influxDB.ShowMeasurements(db)
		if err != nil {
			c.logger.Error(
				"Failed to list measurements",
				zap.Error(err),
			)
			return nil, errors.New("failed to list measurements")
		}

		for _, m := range measurements {
			if db == key.DB && m == key.Measurement {
				return &responsetypes.KeyGet{Status: responsetypes.KeyPresent}, nil
			}
		}
	}

	return &responsetypes.KeyGet{Status: responsetypes.KeyAbsent}, nil
}

// run in the background and periodically ping all replicas that should own the same keys this node holds to confirm
// they have it; if they don't, start to transfer them
//
// keys will be missing when nodes are added or replaced
//
// keeping it running continuously ensures missing keys will always be transferred (given enough time) even if a node
// initiates and then stops/fails midway through it
//func (d *Chronos) transferKeys() {
//	// block indefinitely while the node is initializing
//	for d.initializing {
//		d.logger.Debug("Delaying key transfer while initializing")
//		time.Sleep(time.Second * 3)
//	}
//
//	d.logger.Info("Starting background task for transferring keys")
//	for {
//		// don't transfer if receiving
//		for d.keyRecvPending(nil) || d.recovering() {
//			d.logger.Debug("Delaying key transfer while recovering and/or receiving keys")
//			time.Sleep(time.Second * 3)
//		}
//		// all nodes that are missing a given key
//		targets, err := d.fsmTransferCollectTargets()
//		if err != nil {
//			// failing here is weird but probably not a strong enough reason to terminate
//			d.logger.Error("Failed to collect targets for key transfer", zap.Error(err))
//		} else {
//			// the order by which target nodes are processed will be random (it's a map), which is very convenient to
//			// minimize the probability of having more than one node trying to transfer data to the same target
//			for node, keys := range targets {
//				d.fsmTransferAllKeys(node, keys)
//			}
//		}
//
//		// wait for a random period of time to avoid having multiple nodes trying to initiate transfer requests at
//		// the same time
//		r := rand.Intn(d.cfg.CrossCheckSendInterval)
//		d.logger.Debug("Sleeping between key transfer attempts", zap.Int("time", r))
//		time.Sleep(time.Duration(r) * time.Second)
//	}
//}

func (c *Chronos) forwardRequest(
	node string,
	headers *http.Header,
	uri string,
	key *coretypes.Key,
	payload []byte,
) (int, []byte) {
	return c.request.Forward(c.cfg.NodeID, headers, uri, key, payload)
}

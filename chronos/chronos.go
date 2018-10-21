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

	// all of the following need to be background tasks so that New()
	// returns immediately, leaving ChronosDB operating and accepting requests

	// initialize the node -- create all DBs, users, ...
	go c.initialize()
	// continuously replay all entries in the intent log
	go c.replayIntentLog()
	// TODO: this could/should be something that's explicitly called when a new node is added or a request to
	// rebalance comes in
	// // background task to push local keys to other nodes
	// go chronos.transferKeys()
	// background task to check for the last write in recover mode and exit it if past the grace period (and not
	// initializing)
	go c.checkAndExitRecoveryMode()
}

// continuously query the Intent Log for available entries and try to replay them
func (c *Chronos) replayIntentLog() {
	c.logger.Info("Starting to replay the Intent Log")
	wait := c.cfg.ReplayInterval
	forwardWriteResultsChan := make(chan fsmForwardWriteResult, 1)

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

		c.fsmForwardWrite(
			entry.Node,
			entry.URI,
			entry.Key,
			entry.Payload,
			forwardWriteResultsChan,
		)
		result := <-forwardWriteResultsChan
		if result.httpStatus >= 200 && result.httpStatus <= 299 {
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
	if c.nodeIsCoordinator(headers) {
		var status int
		var response []byte

		for _, node := range c.cluster.GetAllNodes() {
			if node != c.cfg.NodeID {
				u := c.createForwardURL(node, uri)
				h := createForwardHeaders(nil)
				c.logger.Debug("Forwarding request", zap.String("url", u), zap.String("action", action))
				switch action {
				case "DROP":
					status, response = shared.DoDelete(
						u,
						nil,
						h,
						c.httpClient,
						c.cfg.MaxRetries,
						c.logger,
						"chronos.DropDB",
					)
				case "CREATE":
					status, response = shared.DoPut(
						u,
						nil,
						h,
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

	// if dealing with a hinted handoff or key transfer we need to put the node in recovery mode
	c.checkAndSetRecoveryMode(form)

	// if a key is being transferred, update the tracking information
	if c.requestIsKeyTransfer(form) {
		// persist this to disk every to often so that we can survive a temporary failure during the transfer
		//
		// problem: transfer begins, receiving node dies midway through, sender node fails while generating intentLog; next
		// time this node is queried it'll say the key exists even though it's incomplete
		// TODO
		//k := c.getKeyFromURL(form)
		//c.beginKeyRecv(k)
		//c.keyRecvLock.Lock()
		//c.keyRecvTimestamp[k] = time.Now()
		//c.keyRecvLock.Unlock()
	}

	return c.fsmStartWrite(headers, uri, form, payload)
}

func (c *Chronos) Query(headers http.Header, uri string, form url.Values) (int, []byte) {
	// no querying metrics while initializing
	if c.initializing {
		return http.StatusServiceUnavailable, []byte("initializing")
	}

	return c.fsmStartQuery(headers, uri, form)
}

// remove the marker that indicates a key transfer is in progress
func (c *Chronos) KeyRecvCompleted(key *coretypes.Key) error {
	// TODO
	// return c.endKeyRecv(key)
	return nil
}

// TODO: cache results (when safe)
func (c *Chronos) DoesKeyExist(key *coretypes.Key) (bool, error) {
	// TODO
	return true, nil

	//// if a transfer is already in progress return true so that another one does not start
	//if c.keyRecvInProgress(key) {
	//	c.logger.Debug("Recv in progress", zap.String("key", key.String()))
	//	return true, nil
	//}
	//
	//// if a transfer is not in progress but was started at some point and never completed, return false right now so
	//// that it can restart (otherwise the check below would just return true and we have only part of the data)
	//if c.keyRecvPending(key) {
	//	c.logger.Debug("Recv pending", zap.String("key", key.String()))
	//	return false, nil
	//}
	//
	//dbs, err := c.influxDB.ShowDBs()
	//if err != nil {
	//	c.logger.Error(
	//		"Failed to list DBs",
	//		zap.Error(err),
	//	)
	//	return false, errors.New("failed to list databases")
	//}
	//
	//for _, db := range dbs {
	//	measurements, err := c.influxDB.ShowMeasurements(db)
	//	if err != nil {
	//		c.logger.Error(
	//			"Failed to list measurements",
	//			zap.Error(err),
	//		)
	//		return false, errors.New("failed to list measurements")
	//	}
	//
	//	for _, m := range measurements {
	//		if db == key.DB && m == key.Measurement {
	//			return true, nil
	//		}
	//	}
	//}
	//
	//return false, nil
}

// return true iff in recovery
func (c *Chronos) isRecovering(key *coretypes.Key) bool {
	// don't even try to access the map
	if key == nil {
		return false
	}

	c.recoveryLock.RLock()
	_, ok := c.recovering[key]
	c.recoveryLock.RUnlock()

	return ok
}

// if the request is either a hint being handed off or part of a key transfer, put the node in recovery mode and
// update the timestamp (for the key being written)
func (c *Chronos) checkAndSetRecoveryMode(form url.Values) {
	if c.requestIsHintedHandoff(form) || c.requestIsKeyTransfer(form) {
		// both hinted hand offs and key transfers include the key name in the query string, so this is safe
		key := getKeyFromURL(form)
		c.logger.Info("Putting node in recovery mode", zap.String("key", key.String()))
		c.recoveryLock.Lock()
		c.recovering[key] = time.Now()
		c.recoveryLock.Unlock()
	}
}

// run in the background, continuously checking for the latest recovery timestamp; exit recovery mode if
// RecoveryGracePeriod seconds or more have passed since the last entry in the intent log was replayed
func (c *Chronos) checkAndExitRecoveryMode() {
	c.logger.Info("Starting background task for exiting recovery mode")

	for {
		done := make([]*coretypes.Key, 0)

		c.recoveryLock.RLock()
		for k, t := range c.recovering {
			if time.Since(t) >= (time.Second * time.Duration(c.cfg.RecoveryGracePeriod)) {
				done = append(done, k)
			}
		}
		c.recoveryLock.RUnlock()

		for _, k := range done {
			c.logger.Debug("Exiting recovery mode", zap.String("key", k.String()))
			c.recoveryLock.Lock()
			delete(c.recovering, k)
			c.recoveryLock.Unlock()
		}

		c.logger.Debug("Sleeping for checking recovery mode", zap.Int("time", c.cfg.RecoveryGracePeriod))
		time.Sleep(time.Second * time.Duration(c.cfg.RecoveryGracePeriod))
	}
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
//		r := rand.Intn(d.cfg.KeyTransferInterval)
//		d.logger.Debug("Sleeping between key transfer attempts", zap.Int("time", r))
//		time.Sleep(time.Duration(r) * time.Second)
//	}
//}

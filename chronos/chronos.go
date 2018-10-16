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
	intentLog  *ilog.Cfg
	httpClient *http.Client
	influxDB   *influxdb.InfluxDB
	// node is still initializing
	isInitializing bool
	// receiving data from a hinted handoff or key transfer
	inRecovery map[*coretypes.Key]time.Time
	// lastRecoveryTimestamp map[*coretypes.Key]time.Time
	recoveryLock sync.RWMutex
	//// keep track of keys being transferred
	//keyRecvTimestamp map[*coretypes.Key]time.Time
	//keyRecvLock      sync.RWMutex
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
		cfg:       cfg,
		cluster:   cluster,
		gossip:    gossip.New(cfg.Port, httpClient, cfg.MaxRetries, logger),
		intentLog: ilog.New(cfg.DataDirectory, logger),
		// always start in recovery mode to make sure we give it enough time for intentLog to start replaying or
		// key transfer operations to begin before accepting queries
		// the recovery grace period should be (2x?) higher than the intentLog handoff interval to make sure we don't exit
		// recovery mode *before* intentLog start to be replayed
		isInitializing: true,
		inRecovery:     make(map[*coretypes.Key]time.Time, 0),
		// lastRecoveryTimestamp: make(map[*coretypes.Key]time.Time, 0),
		// TODO: isn't there a cleaner way of doing this?
		// // keep track of key transfer operations
		// keyRecvTimestamp: make(map[*coretypes.Key]time.Time, 0),
		// TODO: this should be part of the bootstrap process and probably not even in the struct
		// // if the node is not initializing, the first function call will update this value
		// isInitializing: true,
		logger:     logger,
		httpClient: httpClient,
		influxDB:   influxdb.New(&cfg.InfluxDB, logger),
	}
}

func (d *Chronos) Start() {
	// cleanup intentLog: make sure we don't have any dangling intentLog (i.e., handoff started but was interrupted) and
	// delete stale intentLog (i.e., intended for nodes that are no longer part of the cluster)
	d.intentLog.RestoreDangling()
	d.intentLog.RemoveStale(d.cluster.GetAllNodes())

	// initialize the node -- create all DBs
	// it needs to be a background task so that New() returns leaving ChronosDB operating in a normal way and
	// accepting all write requests
	//
	// the node stays in recovery mode while it is initializing
	go d.initialize()
	// replay all entries in the intent log continuously in the background
	go d.replayIntentLog()
	// TODO: this could/should be something that's explicitly called when a new node is added or a request to
	// rebalance comes in
	// // background task to push local keys to other nodes
	// go chronos.transferKeys()
	// background task to check for the last write in recover mode and exit it if past the grace period (and not
	// isInitializing)
	go d.checkAndExitRecoveryMode()
}

// continuously query the Intent Log for available entries and try to replay them
func (d *Chronos) replayIntentLog() {
	d.logger.Info("Starting to replay the Intent Log")
	wait := d.cfg.ReplayInterval
	forwardWriteResultsChan := make(chan fsmForwardWriteResult, 1)

	for {
		d.logger.Debug("Sleeping between intent log entries", zap.Int("time", wait))
		time.Sleep(time.Second * time.Duration(wait))
		entry, err := d.intentLog.Fetch()
		if err != nil {
			d.logger.Error("Failed to Fetch an entry from the intent log", zap.Error(err))
			continue
		}

		if entry == nil {
			// no log entries to process, extend the sleep time up to the maximum as set in the configuration file
			if wait == 0 {
				wait = 1
			} else {
				wait *= 2
			}
			if wait > d.cfg.ReplayInterval {
				wait = d.cfg.ReplayInterval
			}
			continue
		} else {
			// there are more log entries to process, reduce the waiting period
			wait /= 2
		}

		d.logger.Info(
			"Processing intent log entry",
			zap.String("node", entry.Node),
			zap.String("key", entry.Key.String()),
			zap.String("uri", entry.URI),
		)

		d.fsmForwardWrite(
			entry.Node,
			entry.URI,
			entry.Key,
			entry.Payload,
			forwardWriteResultsChan,
		)
		result := <-forwardWriteResultsChan
		if result.httpStatus >= 200 && result.httpStatus <= 299 {
			err := d.intentLog.Remove(entry)
			if err != nil {
				d.logger.Error("Failed to remove intent log entry", zap.Error(err))
			}
		} else {
			d.logger.Error("Failed to replay intent log entry",
				zap.String("node", entry.Node),
				zap.String("key", entry.Key.String()),
				zap.String("uri", entry.URI),
			)
			// TODO: a failed node can significantly slow down replaying data to healthy nodes
			// adjust the wait period to avoid unnecessary networking traffic when nodes are not available
			wait *= 2
			if wait > d.cfg.ReplayInterval {
				wait = d.cfg.ReplayInterval
			}
			// save the entry for replaying later
			d.intentLog.SaveForRerun(entry)
		}
	}
}

func (d *Chronos) NodeStatus() *responsetypes.NodeStatus {
	return &responsetypes.NodeStatus{
		Initializing: d.isInitializing,
		Recovering:   d.inRecovery,
	}
}

func (d *Chronos) GetCluster() *responsetypes.GetRing {
	return &responsetypes.GetRing{Nodes: d.cluster.GetAllNodes()}
}

// func (dyn *Chronos) GetDBs() ([]string, error) {
func (d *Chronos) GetDBs() (*responsetypes.GetDBs, error) {
	// even in recovery mode should be safe to return the list of DBs as only metrics are replayed
	dbs, err := d.influxDB.ShowDBs()
	if err != nil {
		d.logger.Error("Failed to list DBs", zap.Error(err))
		// return []string{}, errors.New("failed to get the list of DBs")
		return nil, err
	}

	// return dbs, nil
	return &responsetypes.GetDBs{Databases: dbs}, nil
}

// try to create (or drop) and DB on all nodes in the cluster; on failure return the node that failed to comply
func (d *Chronos) createOrDropDB(uri string, form url.Values, db string, action string) (string, error) {
	var err error

	// regardless of whether or not being a coordinator, create the DB locally
	d.logger.Debug(
		"Creating/Dropping DB",
		zap.String("db", db),
		zap.String("node", d.cfg.NodeID),
		zap.String("action", action),
	)

	switch action {
	case "DROP":
		err = d.influxDB.DropDB(db)
	case "CREATE":
		err = d.influxDB.CreateDB(db)

	}
	if err != nil {
		d.logger.Error(
			"Failed to create/drop DB",
			zap.String("db", db),
			zap.String("action", action),
			zap.String("node", d.cfg.NodeID),
			zap.Error(err),
		)

		return d.cfg.NodeID, errors.New("influxDB failed")
	}

	// if acting as coordinator, i.e., this request was not forwarded, forward it to all nodes (but itself)
	if d.nodeIsCoordinator(form) {
		var status int
		var response []byte

		for _, node := range d.cluster.GetAllNodes() {
			if node != d.cfg.NodeID {
				u := d.createForwardURL(node, uri)
				d.logger.Debug("Forwarding request", zap.String("url", u), zap.String("action", action))
				switch action {
				case "DROP":
					status, response = shared.DoDelete(
						u,
						nil,
						nil,
						d.httpClient,
						d.cfg.MaxRetries,
						d.logger,
						"chronos.DropDB",
					)
				case "CREATE":
					status, response = shared.DoPut(
						u,
						nil,
						nil,
						d.httpClient,
						d.cfg.MaxRetries,
						d.logger,
						"chronos.CreateDB",
					)
				}

				if !(status >= 200 && status <= 299) {
					d.logger.Error(
						"Failed to create/drop DB; rolling back",
						zap.String("db", db),
						zap.String("action", action),
						zap.String("node", d.cfg.NodeID),
						zap.ByteString("httpResponse", response),
						zap.Error(err),
					)
					// try to rollback
					if action == "CREATE" {
						node, err := d.DropDB(uri, form, db)
						if err != nil {
							d.logger.Error("Failed to rollback CREATE DB", zap.String("node", node))
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

func (d *Chronos) CreateDB(uri string, form url.Values, db string) (string, error) {
	return d.createOrDropDB(uri, form, db, "CREATE")
}

func (d *Chronos) DropDB(uri string, form url.Values, db string) (string, error) {
	return d.createOrDropDB(uri, form, db, "DROP")
}

func (d *Chronos) Write(uri string, form url.Values, payload []byte) (int, []byte) {
	// if dealing with a hinted handoff or key transfer we need to put the node in recovery mode
	d.checkAndSetRecoveryMode(form)

	// if a key is being transferred, update the tracking information
	if d.requestIsKeyTransfer(form) {
		// persist this to disk every to often so that we can survive a temporary failure during the transfer
		//
		// problem: transfer begins, receiving node dies midway through, sender node fails while generating intentLog; next
		// time this node is queried it'll say the key exists even though it's incomplete
		// TODO
		//k := d.getKeyFromURL(form)
		//d.beginKeyRecv(k)
		//d.keyRecvLock.Lock()
		//d.keyRecvTimestamp[k] = time.Now()
		//d.keyRecvLock.Unlock()
	}

	return d.fsmStartWrite(uri, form, payload)
}

func (d *Chronos) Query(uri string, form url.Values) (int, []byte) {
	return d.fsmStartQuery(uri, form)
}

// remove the marker that indicates a key transfer is in progress
func (d *Chronos) KeyRecvCompleted(key *coretypes.Key) error {
	// TODO
	// return d.endKeyRecv(key)
	return nil
}

// TODO: cache results (when safe)
func (d *Chronos) DoesKeyExist(key *coretypes.Key) (bool, error) {
	// TODO
	return true, nil

	//// if a transfer is already in progress return true so that another one does not start
	//if d.keyRecvInProgress(key) {
	//	d.logger.Debug("Recv in progress", zap.String("key", key.String()))
	//	return true, nil
	//}
	//
	//// if a transfer is not in progress but was started at some point and never completed, return false right now so
	//// that it can restart (otherwise the check below would just return true and we have only part of the data)
	//if d.keyRecvPending(key) {
	//	d.logger.Debug("Recv pending", zap.String("key", key.String()))
	//	return false, nil
	//}
	//
	//dbs, err := d.influxDB.ShowDBs()
	//if err != nil {
	//	d.logger.Error(
	//		"Failed to list DBs",
	//		zap.Error(err),
	//	)
	//	return false, errors.New("failed to list databases")
	//}
	//
	//for _, db := range dbs {
	//	measurements, err := d.influxDB.ShowMeasurements(db)
	//	if err != nil {
	//		d.logger.Error(
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
func (d *Chronos) isRecovering(key *coretypes.Key) bool {
	d.recoveryLock.RLock()
	_, ok := d.inRecovery[key]
	d.recoveryLock.RUnlock()

	return ok
}

// if the request is either a hint being handed off or part of a key transfer, put the node in recovery mode and
// update the timestamp (for the key being written)
func (d *Chronos) checkAndSetRecoveryMode(form url.Values) {
	if d.requestIsHintedHandoff(form) || d.requestIsKeyTransfer(form) {
		// both hinted hand offs and key transfers include the key name in the query string, so this is safe
		key := d.getKeyFromURL(form)
		d.logger.Info("Putting node in recovery mode", zap.String("key", key.String()))
		d.recoveryLock.Lock()
		d.inRecovery[key] = time.Now()
		d.recoveryLock.Unlock()
	}
}

// run in the background, continuously checking for the latest recovery timestamp; exit recovery mode if
// RecoveryGracePeriod seconds or more have passed since the last hint was replayed
//
// while isInitializing a new node we should also keep it in recovery mode until that task is completed
func (d *Chronos) checkAndExitRecoveryMode() {
	d.logger.Info("Starting background task for exiting recovery mode")

	// block indefinitely while the node is initializing
	for d.isInitializing {
		d.logger.Debug("Delaying exiting recovery mode while initializing")
		time.Sleep(time.Second * 3)
	}

	for {
		done := make([]*coretypes.Key, 0)

		d.recoveryLock.RLock()
		for k, t := range d.inRecovery {
			if time.Since(t) >= (time.Second * time.Duration(d.cfg.RecoveryGracePeriod)) {
				done = append(done, k)
			}
		}
		d.recoveryLock.RUnlock()

		for _, k := range done {
			d.logger.Debug("Exiting recovery mode", zap.String("key", k.String()))
			d.recoveryLock.Lock()
			delete(d.inRecovery, k)
			d.recoveryLock.Unlock()
		}

		d.logger.Debug("Sleeping for checking recovery mode", zap.Int("time", d.cfg.RecoveryGracePeriod))
		time.Sleep(time.Second * time.Duration(d.cfg.RecoveryGracePeriod))
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
//	for d.isInitializing {
//		d.logger.Debug("Delaying key transfer while initializing")
//		time.Sleep(time.Second * 3)
//	}
//
//	d.logger.Info("Starting background task for transferring keys")
//	for {
//		// don't transfer if receiving
//		for d.keyRecvPending(nil) || d.isRecovering() {
//			d.logger.Debug("Delaying key transfer while isRecovering and/or receiving keys")
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

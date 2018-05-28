package dynamo

import (
	"errors"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"github.com/marcoalmeida/chronosdb/hint"
	"github.com/marcoalmeida/chronosdb/influxdb"
	"github.com/marcoalmeida/chronosdb/responsetypes"
	"github.com/marcoalmeida/chronosdb/shared"
	"github.com/marcoalmeida/hrw"
	"go.uber.org/zap"
)

const (
	DefaultDataDirectory        = "/var/lib/chronosdb"
	DefaultNumberReplicas       = 3
	DefaultWriteQuorum          = 2
	DefaultConnectTimeout       = 500
	DefaultClientTimeout        = 3000
	DefaultMaxRetires           = 3
	DefaultHandoffInterval      = 15
	DefaultKeyTransferInterval  = 30
	DefaultKeyTransferChunkSize = 10000
	DefaultKeyRecvTimeout       = 60
)

type Cfg struct {
	DataDirectory        string             `toml:"data_dir"`
	Nodes                map[string]float64 `toml:"nodes"`
	NodeID               string             `toml:"node_id"`
	NumberOfReplicas     int                `toml:"n_replicas"`
	WriteQuorum          int                `toml:"write_quorum"`
	HandoffInterval      int                `toml:"handoff_interval"`
	KeyTransferInterval  int                `toml:"key_transfer_interval"`
	KeyTransferBatchSize int                `toml:"key_transfer_batch_size"`
	KeyRecvTimeout       int                `toml:"key_recv_timeout"`
	RecoveryGracePeriod  int                `toml:"recovery_grace_period"`
	ConnectTimeout       int                `toml:"connect_timeout"`
	ClientTimeout        int                `toml:"client_timeout"`
	MaxRetries           int                `toml:"max_retries"`
}

type Dynamo struct {
	cfg           *Cfg
	chronosDBPort int64
	logger        *zap.Logger
	ring          hrw.Nodes
	hints         *hint.Cfg
	httpClient    *http.Client
	influxDB      *influxdb.InfluxDB
	// receiving data from a hinted handoff or key transfer
	inRecovery bool
	// node is still initializing
	isInitializing        bool
	lastRecoveryTimestamp time.Time
	recoveryLock          sync.RWMutex
	// keep track of keys being transferred
	keyRecvTimestamp map[*coretypes.Key]time.Time
	keyRecvLock      sync.RWMutex
}

func New(chronosDBPort int64, dynamoCFG *Cfg, influxDBCFG *influxdb.Cfg, logger *zap.Logger) *Dynamo {
	// create the ring with all nodes listed in the configuration file
	ring := hrw.New(nil)
	for node, weight := range dynamoCFG.Nodes {
		logger.Debug("Adding node to the ring", zap.String("node", node), zap.Float64("weight", weight))
		ring.AddNode(hrw.Node{Name: node, Weight: weight})
	}

	hints := hint.New(dynamoCFG.DataDirectory, logger)
	// make sure we don't have any dangling hints, i.e., handoff started but was interrupted
	hints.Recover()
	// delete stale hints, i.e., intended for nodes that are no longer part of the ring
	hints.RemoveStale(ring.GetAllNodes())

	// dynamo instance to return
	dynamo := &Dynamo{
		cfg:           dynamoCFG,
		chronosDBPort: chronosDBPort,
		ring:          ring,
		hints:         hints,
		// always start in recovery mode to make sure we give it enough time for hints to start replaying or
		// key transfer operations to begin before accepting queries
		// the recovery grace period should be (2x?) higher than the hints handoff interval to make sure we don't exit
		// recovery mode *before* hints start to be replayed
		inRecovery:            true,
		lastRecoveryTimestamp: time.Now(),
		// keep track of key transfer operations
		keyRecvTimestamp: make(map[*coretypes.Key]time.Time, 0),
		// if the node is not initializing, the first function call will update this value
		isInitializing: true,
		logger:         logger,
		httpClient:     shared.NewHTTPClient(dynamoCFG.ConnectTimeout, dynamoCFG.ClientTimeout),
		influxDB:       influxdb.New(influxDBCFG, logger),
	}

	// make sure InfluxDB is up and running -- no point on proceeding otherwise
	if !dynamo.influxDB.IsAlive() {
		dynamo.logger.Fatal("Failed to connect to InfluxDB")
	}

	// initialize the node -- create all DBs
	// it needs to be a background task so that New() returns leaving ChronosDB operating in a normal way and
	// accepting all write requests
	//
	// the node stays in recovery mode while it is initializing
	go dynamo.initialize()
	// background task to replay locally stored hints
	go dynamo.replayHints()
	// background task to push local keys to other nodes
	go dynamo.transferKeys()
	// background task to check for the last write in recover mode and exit it if past the grace period (and not
	// isInitializing)
	go dynamo.checkAndExitRecoveryMode()

	return dynamo
}

func (dyn *Dynamo) NodeStatus() *responsetypes.NodeStatus {
	return &responsetypes.NodeStatus{
		Initializing: dyn.isInitializing,
		Recovering:   dyn.recovering(),
	}
}

func (dyn *Dynamo) GetRing() *responsetypes.GetRing {
	return &responsetypes.GetRing{Nodes: dyn.ring.GetAllNodes()}
}

// func (dyn *Dynamo) GetDBs() ([]string, error) {
func (dyn *Dynamo) GetDBs() (*responsetypes.GetDBs, error) {
	// even in recovery mode should be safe to return the list of DBs as only metrics are replayed
	dbs, err := dyn.influxDB.ShowDBs()
	if err != nil {
		dyn.logger.Error("Failed to list DBs", zap.Error(err))
		// return []string{}, errors.New("failed to get the list of DBs")
		return nil, err
	}

	// return dbs, nil
	return &responsetypes.GetDBs{Databases: dbs}, nil
}

// try to create (or drop) and DB on all nodes in the ring; on failure return the node that failed to comply
func (dyn *Dynamo) createOrDropDB(uri string, form url.Values, db string, action string) (string, error) {
	var err error

	// regardless of whether or not being a coordinator, create the DB locally
	dyn.logger.Debug(
		"Creating/Dropping DB",
		zap.String("db", db),
		zap.String("node", dyn.cfg.NodeID),
		zap.String("action", action),
	)

	switch action {
	case "DROP":
		err = dyn.influxDB.DropDB(db)
	case "CREATE":
		err = dyn.influxDB.CreateDB(db)

	}
	if err != nil {
		dyn.logger.Error(
			"Failed to create/drop DB",
			zap.String("db", db),
			zap.String("action", action),
			zap.String("node", dyn.cfg.NodeID),
			zap.Error(err),
		)

		return dyn.cfg.NodeID, errors.New("influxDB failed")
	}

	// if acting as coordinator, i.e., this request was not forwarded, forward it to all nodes (but itself)
	if dyn.nodeIsCoordinator(form) {
		var status int
		var response []byte

		for _, node := range dyn.ring.GetAllNodes() {
			if node != dyn.cfg.NodeID {
				u := dyn.createForwardURL(node, uri)
				dyn.logger.Debug("Forwarding request", zap.String("url", u), zap.String("action", action))
				switch action {
				case "DROP":
					status, response = shared.DoDelete(
						u,
						nil,
						nil,
						dyn.httpClient,
						dyn.cfg.MaxRetries,
						dyn.logger,
						"dynamo.DropDB",
					)
				case "CREATE":
					status, response = shared.DoPut(
						u,
						nil,
						nil,
						dyn.httpClient,
						dyn.cfg.MaxRetries,
						dyn.logger,
						"dynamo.CreateDB",
					)
				}

				if !(status >= 200 && status <= 299) {
					dyn.logger.Error(
						"Failed to create/drop DB; rolling back",
						zap.String("db", db),
						zap.String("action", action),
						zap.String("node", dyn.cfg.NodeID),
						zap.ByteString("response", response),
						zap.Error(err),
					)
					// try to rollback
					if action == "CREATE" {
						node, err := dyn.DropDB(uri, form, db)
						if err != nil {
							dyn.logger.Error("Failed to rollback CREATE DB", zap.String("node", node))
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

func (dyn *Dynamo) CreateDB(uri string, form url.Values, db string) (string, error) {
	return dyn.createOrDropDB(uri, form, db, "CREATE")
}

func (dyn *Dynamo) DropDB(uri string, form url.Values, db string) (string, error) {
	return dyn.createOrDropDB(uri, form, db, "DROP")
}

func (dyn *Dynamo) Write(uri string, form url.Values, payload []byte) (int, []byte) {
	// if dealing with a hinted handoff or key transfer we need to put the node in recovery mode
	dyn.checkAndSetRecoveryMode(form)

	// if a key is being transferred, update the tracking information
	if dyn.requestIsKeyTransfer(form) {
		// persist this to disk every to often so that we can survive a temporary failure during the transfer
		//
		// problem: transfer begins, receiving node dies midway through, sender node fails while generating hints; next
		// time this node is queried it'll say the key exists even though it's incomplete
		k := dyn.getKeyFromURL(form)
		dyn.beginKeyRecv(k)
		dyn.keyRecvLock.Lock()
		dyn.keyRecvTimestamp[k] = time.Now()
		dyn.keyRecvLock.Unlock()
	}

	return dyn.fsmStartWrite(uri, form, payload)
}

func (dyn *Dynamo) Query(uri string, form url.Values) (int, []byte) {
	// don't respond to queries if recovering and not coordinating -- nodes recovering are still able to
	// coordinate/forward requests
	if dyn.recovering() && !dyn.nodeIsCoordinator(form) {
		return http.StatusServiceUnavailable, []byte("in recovery")
	}

	return dyn.fsmStartQuery(uri, form)
}

// remove the marker that indicates a key transfer is in progress
func (dyn *Dynamo) KeyRecvCompleted(key *coretypes.Key) error {
	return dyn.endKeyRecv(key)
}

// TODO: cache results (when safe)
func (dyn *Dynamo) DoesKeyExist(key *coretypes.Key) (bool, error) {
	// if a transfer is already in progress return true so that another one does not start
	if dyn.keyRecvInProgress(key) {
		dyn.logger.Debug("Recv in progress", zap.String("key", key.String()))
		return true, nil
	}

	// if a transfer is not in progress but was started at some point and never completed, return false right now so
	// that it can restart (otherwise the check below would just return true and we have only part of the data)
	if dyn.keyRecvPending(key) {
		dyn.logger.Debug("Recv pending", zap.String("key", key.String()))
		return false, nil
	}

	dbs, err := dyn.influxDB.ShowDBs()
	if err != nil {
		dyn.logger.Error(
			"Failed to list DBs",
			zap.Error(err),
		)
		return false, errors.New("failed to list databases")
	}

	for _, db := range dbs {
		measurements, err := dyn.influxDB.ShowMeasurements(db)
		if err != nil {
			dyn.logger.Error(
				"Failed to list measurements",
				zap.Error(err),
			)
			return false, errors.New("failed to list measurements")
		}

		for _, m := range measurements {
			if db == key.DB && m == key.Measurement {
				return true, nil
			}
		}
	}

	return false, nil
}

// return true iff in recovery
func (dyn *Dynamo) recovering() bool {
	dyn.recoveryLock.RLock()
	r := dyn.inRecovery
	dyn.recoveryLock.RUnlock()

	return r
}

// if the request is either a hint being handed off or part of a key transfer, put the node in recovery mode
// and update the timestamp
func (dyn *Dynamo) checkAndSetRecoveryMode(form url.Values) {
	if dyn.requestIsHintedHandoff(form) || dyn.requestIsKeyTransfer(form) {
		dyn.logger.Info("Putting node in recovery mode")
		dyn.recoveryLock.Lock()
		dyn.inRecovery = true
		dyn.lastRecoveryTimestamp = time.Now()
		dyn.recoveryLock.Unlock()
	}
}

// run in the background, continuously checking for the latest recovery timestamp; exit recovery mode if
// RecoveryGracePeriod seconds or more have passed since the last hint was replayed
//
// while isInitializing a new node we should also keep it in recovery mode until that task is completed
func (dyn *Dynamo) checkAndExitRecoveryMode() {
	dyn.logger.Info("Starting background task for exiting recovery mode")

	// block indefinitely while the node is initializing
	for dyn.isInitializing {
		dyn.logger.Debug("Delaying exiting recovery mode while initializing")
		time.Sleep(time.Second * 3)
	}

	for {
		dyn.recoveryLock.RLock()
		recovering := dyn.inRecovery
		lastWrite := dyn.lastRecoveryTimestamp
		dyn.recoveryLock.RUnlock()

		if recovering {
			if time.Since(lastWrite) >= (time.Second * time.Duration(dyn.cfg.RecoveryGracePeriod)) {
				dyn.logger.Debug("Exiting recovery mode")
				dyn.recoveryLock.Lock()
				dyn.inRecovery = false
				dyn.recoveryLock.Unlock()
			}
		}

		dyn.logger.Debug("Sleeping for checking recovery mode", zap.Int("time", dyn.cfg.RecoveryGracePeriod))
		time.Sleep(time.Second * time.Duration(dyn.cfg.RecoveryGracePeriod))
	}
}

// run in the background, continuously reading the hints directory structure and replaying its contents
func (dyn *Dynamo) replayHints() {
	dyn.logger.Info("Starting background task for hinted handoffs")
	wait := dyn.cfg.HandoffInterval
	forwardWriteResultsChan := make(chan fsmForwardWriteResult, 1)

	for {
		dyn.logger.Debug("Sleeping between replaying hints", zap.Int("time", wait))
		time.Sleep(time.Second * time.Duration(wait))
		hint, err := dyn.hints.Fetch()
		if err != nil {
			dyn.logger.Error("Failed to fetch hint", zap.Error(err))
			continue
		}

		// no hints to process, adjust the sleep time and move on
		if hint == nil {
			wait *= 2
			if wait > dyn.cfg.HandoffInterval {
				wait = dyn.cfg.HandoffInterval
			}
			continue
		} else {
			wait /= 2
			if wait == 0 {
				wait = 1
			}
		}

		dyn.logger.Info(
			"Processing hinted handoff",
			zap.String("node", hint.Node),
			zap.String("key", hint.Key.String()),
			zap.String("uri", hint.URI),
		)

		dyn.fsmForwardWrite(
			hint.Node,
			hint.URI,
			hint.Key,
			hint.Payload,
			forwardWriteResultsChan,
		)

		result := <-forwardWriteResultsChan
		if result.httpStatus >= 200 && result.httpStatus <= 299 {
			err := dyn.hints.Remove(hint)
			if err != nil {
				dyn.logger.Error("Failed to remove replayed hint", zap.Error(err))
			}
		} else {
			dyn.logger.Error("Failed to replay hint",
				zap.String("node", hint.Node),
				zap.String("key", hint.Key.String()),
				zap.String("uri", hint.URI),
			)
			// adjust the wait period to avoid unnecessary networking traffic when nodes are not available
			wait *= 2
			if wait > dyn.cfg.HandoffInterval {
				wait = dyn.cfg.HandoffInterval
			}
			// save the hint for replaying later
			dyn.hints.Return(hint)
		}
	}
}

// run in the background and periodically ping all replicas that should own the same keys this node holds to confirm
// they have it; if they don't, start to transfer them
//
// keys will be missing when nodes are added or replaced
//
// keeping it running continuously ensures missing keys will always be transferred (given enough time) even if a node
// initiates and then stops/fails midway through it
func (dyn *Dynamo) transferKeys() {
	// block indefinitely while the node is initializing
	for dyn.isInitializing {
		dyn.logger.Debug("Delaying key transfer while initializing")
		time.Sleep(time.Second * 3)
	}

	dyn.logger.Info("Starting background task for transferring keys")
	for {
		// don't transfer if receiving
		for dyn.keyRecvPending(nil) || dyn.recovering() {
			dyn.logger.Debug("Delaying key transfer while recovering and/or receiving keys")
			time.Sleep(time.Second * 3)
		}
		// all nodes that are missing a given key
		targets, err := dyn.fsmTransferCollectTargets()
		if err != nil {
			// failing here is weird but probably not a strong enough reason to terminate
			dyn.logger.Error("Failed to collect targets for key transfer", zap.Error(err))
		} else {
			// the order by which target nodes are processed will be random (it's a map), which is very convenient to
			// minimize the probability of having more than one node trying to transfer data to the same target
			for node, keys := range targets {
				dyn.fsmTransferAllKeys(node, keys)
			}
		}

		// wait for a random period of time to avoid having multiple nodes trying to initiate transfer requests at
		// the same time
		r := rand.Intn(dyn.cfg.KeyTransferInterval)
		dyn.logger.Debug("Sleeping between key transfer attempts", zap.Int("time", r))
		time.Sleep(time.Duration(r) * time.Second)
	}
}

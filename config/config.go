// TODO: should config be an independent package? why not make config parsing part of main?
package config

import (
	"errors"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/marcoalmeida/chronosdb/influxdb"
	"go.uber.org/zap"
)

const (
	defaultListenIP             = "0.0.0.0"
	defaultPort                 = 8989
	defaultDebug                = false
	defaultDataDirectory        = "/var/lib/chronosdb"
	defaultNumberReplicas       = 3
	defaultWriteQuorum          = 2
	defaultConnectTimeout       = 500
	defaultClientTimeout        = 3000
	defaultMaxRetires           = 3
	defaultHandoffInterval      = 15
	defaultKeyTransferInterval  = 30
	defaultKeyTransferChunkSize = 10000
	defaultKeyRecvTimeout       = 60
)

// possible locations for the configuration file, in order ot preference
var defaultCfgPaths []string = []string{"/etc/chronosdb.toml", "chronosdb.toml"}

type ChronosCfg struct {
	ListenIP             string `toml:"listen_ip"`
	Port                 int64
	EnableDebug          bool               `toml:"enable_debug"`
	DataDirectory        string             `toml:"data_dir"`
	Nodes                map[string]float64 `toml:"nodes"`
	NodeID               string             `toml:"node_id"`
	NumberOfReplicas     int                `toml:"n_replicas"`
	WriteQuorum          int                `toml:"write_quorum"`
	ReadQuorum           int                `toml:"read_quorum"`
	ReplayInterval       int                `toml:"handoff_interval"`
	KeyTransferInterval  int                `toml:"key_transfer_interval"`
	KeyTransferBatchSize int                `toml:"key_transfer_batch_size"`
	KeyRecvTimeout       int                `toml:"key_recv_timeout"`
	RecoveryGracePeriod  int                `toml:"recovery_grace_period"`
	ConnectTimeout       int                `toml:"connect_timeout"`
	ClientTimeout        int                `toml:"client_timeout"`
	MaxRetries           int                `toml:"max_retries"`
	InfluxDB             influxdb.Cfg
}

func New(path *string, logger *zap.Logger) (*ChronosCfg, error) {
	var cfg ChronosCfg

	setDefaults(&cfg)

	// if we got an explicit path to a configuration file we should just use it instead and not even try any of the
	// default places; if for some reason loading it fails, the user just needs to know
	if *path != "" {
		defaultCfgPaths = []string{*path}
	}

	for _, path := range defaultCfgPaths {
		// there's no point on trying to parse a non-existent file
		if _, err := os.Stat(path); err == nil {
			logger.Info("Parsing configuration file", zap.String("path", path))
			if _, err := toml.DecodeFile(path, &cfg); err == nil {
				// validate the final configuration
				err = validateConfig(&cfg)
				if err != nil {
					return nil, err
				} else {
					return &cfg, nil
				}

			} else {
				return nil, errors.New("failed to parse configuration file " + path + ": " + err.Error())
			}
		} else {
			logger.Info("Configuration file not found", zap.String("path", path))
		}
	}

	return nil, errors.New("no valid configuration files found")
}

func setDefaults(cfg *ChronosCfg) {
	// main
	cfg.ListenIP = defaultListenIP
	cfg.Port = defaultPort
	cfg.EnableDebug = defaultDebug
	cfg.DataDirectory = defaultDataDirectory
	cfg.NumberOfReplicas = defaultNumberReplicas
	cfg.WriteQuorum = defaultWriteQuorum
	cfg.ReplayInterval = defaultHandoffInterval
	cfg.KeyTransferInterval = defaultKeyTransferInterval
	cfg.KeyTransferBatchSize = defaultKeyTransferChunkSize
	cfg.KeyRecvTimeout = defaultKeyRecvTimeout
	cfg.ConnectTimeout = defaultConnectTimeout
	cfg.ClientTimeout = defaultClientTimeout
	cfg.MaxRetries = defaultMaxRetires
	// influxdb
	cfg.InfluxDB.Port = influxdb.DefaultPort
	cfg.InfluxDB.ConnectTimeout = influxdb.DefaultConnectTimeout
	cfg.InfluxDB.ClientTimeout = influxdb.DefaultClientTimeout
	cfg.InfluxDB.MaxRetries = influxdb.DefaultMaxRetries
}

func validateConfig(cfg *ChronosCfg) error {
	// make sure the node ID is part of the list of nodes
	validNodeID := false
	for node := range cfg.Nodes {
		if cfg.NodeID == node {
			validNodeID = true
			break
		}
	}
	if !validNodeID {
		return errors.New("node_id not found in dynamo.nodes")
	}

	if cfg.WriteQuorum > cfg.NumberOfReplicas {
		return errors.New("number of replicas must be less or equal to the qrite quorum")
	}

	// the recovery grace period should be 2x higher than the hints handoff interval and the key transfer
	// interval to make sure we don't exit recovery mode *before* hints start to be replayed
	if !((cfg.ReplayInterval * 2) <= cfg.RecoveryGracePeriod) {
		return errors.New("the recovery grace period should be at least 2x larger than the handoff interval")
	}

	if !((cfg.KeyTransferInterval * 2) <= cfg.RecoveryGracePeriod) {
		return errors.New("the recovery grace period should be at least 2x larger than the key transfer interval")
	}

	return nil
}

// TODO: should config be an independent package? why not make config parsing part of main?
package config

import (
	"errors"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/marcoalmeida/chronosdb/dynamo"
	"github.com/marcoalmeida/chronosdb/influxdb"
	"go.uber.org/zap"
)

const (
	defaultListenIP = "0.0.0.0"
	defaultPort     = 8989
	defaultDebug    = false
)

// possible locations for the configuration file, in order ot preference
var defaultCfgPaths []string = []string{"/etc/chronosdb.toml", "chronosdb.toml"}

type MainCfg struct {
	ListenIP    string `toml:"listen_ip"`
	Port        int64
	EnableDebug bool `toml:"enable_debug"`
	Dynamo      dynamo.Cfg
	InfluxDB    influxdb.Cfg
}

func New(path *string, logger *zap.Logger) (*MainCfg, error) {
	var cfg MainCfg

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

func setDefaults(cfg *MainCfg) {
	// main
	cfg.ListenIP = defaultListenIP
	cfg.Port = defaultPort
	cfg.EnableDebug = defaultDebug
	// dynamo
	cfg.Dynamo.DataDirectory = dynamo.DefaultDataDirectory
	cfg.Dynamo.NumberOfReplicas = dynamo.DefaultNumberReplicas
	cfg.Dynamo.WriteQuorum = dynamo.DefaultWriteQuorum
	cfg.Dynamo.HandoffInterval = dynamo.DefaultHandoffInterval
	cfg.Dynamo.KeyTransferInterval = dynamo.DefaultKeyTransferInterval
	cfg.Dynamo.KeyTransferBatchSize = dynamo.DefaultKeyTransferChunkSize
	cfg.Dynamo.KeyRecvTimeout = dynamo.DefaultKeyRecvTimeout
	cfg.Dynamo.ConnectTimeout = dynamo.DefaultConnectTimeout
	cfg.Dynamo.ClientTimeout = dynamo.DefaultClientTimeout
	cfg.Dynamo.MaxRetries = dynamo.DefaultMaxRetires
	// influxdb
	cfg.InfluxDB.Port = influxdb.DefaultPort
	cfg.InfluxDB.ConnectTimeout = influxdb.DefaultConnectTimeout
	cfg.InfluxDB.ClientTimeout = influxdb.DefaultClientTimeout
	cfg.InfluxDB.MaxRetries = influxdb.DefaultMaxRetries
}

func validateConfig(cfg *MainCfg) error {
	// make sure the node ID is part of the list of nodes
	validNodeID := false
	for node := range cfg.Dynamo.Nodes {
		if cfg.Dynamo.NodeID == node {
			validNodeID = true
			break
		}
	}
	if !validNodeID {
		return errors.New("node_id not found in dynamo.nodes")
	}

	if cfg.Dynamo.WriteQuorum > cfg.Dynamo.NumberOfReplicas {
		return errors.New("number of replicas must be less or equal to the qrite quorum")
	}

	// the recovery grace period should be 2x higher than the hints handoff interval and the key transfer
	// interval to make sure we don't exit recovery mode *before* hints start to be replayed
	if !((cfg.Dynamo.HandoffInterval * 2) <= cfg.Dynamo.RecoveryGracePeriod) {
		return errors.New("the recovery grace period should be at least 2x larger than the handoff interval")
	}

	if !((cfg.Dynamo.KeyTransferInterval * 2) <= cfg.Dynamo.RecoveryGracePeriod) {
		return errors.New("the recovery grace period should be at least 2x larger than the key transfer interval")
	}

	return nil
}

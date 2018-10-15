package main

import (
	"flag"
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/marcoalmeida/chronosdb/config"
	"github.com/marcoalmeida/chronosdb/dynamo"
	"github.com/marcoalmeida/chronosdb/influxdb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type appCfg struct {
	cfg    *config.MainCfg
	logger *zap.Logger
	dynamo *dynamo.Dynamo
}

// TODO: is rand still used anywhere?
func init() {
	rand.Seed(time.Now().UnixNano())
}

func setupLogging() (*zap.Logger, *zap.AtomicLevel) {
	atom := zap.NewAtomicLevel()
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	return zap.New(zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderCfg),
			zapcore.Lock(os.Stdout),
			atom),
		),
		&atom
}

func mustParseConfig(logger *zap.Logger) *config.MainCfg {
	var cfgFile = flag.String("cfg", "", "path to the configuration file")
	flag.Parse()

	// parse the main configuration file (will default to standard locations if no file was set using the flag above)
	cfg, err := config.New(cfgFile, logger)
	if err != nil {
		logger.Fatal("Configuration error:", zap.Error(err))
	}

	return cfg
}

// block until InfluxDB is up and running
func waitForInfluxDB(cfg *influxdb.Cfg, logger *zap.Logger) {
	idb := influxdb.New(cfg, logger)
	for !idb.IsAlive() {
		logger.Debug("Waiting for InfluxDB to report being alive and healthy...")
		time.Sleep(time.Duration(3) * time.Second)
	}
}

func main() {
	// logging
	logger, atom := setupLogging()
	// flush the buffer before exiting
	defer logger.Sync()

	// parse the configuration file
	cfg := mustParseConfig(logger)
	// we can now set the requested log level
	if cfg.EnableDebug {
		atom.SetLevel(zap.DebugLevel)
	}
	logger.Debug("Application configuration", zap.String("options", fmt.Sprintf("%+v", cfg)))

	// make sure InfluxDB is up and running -- there's no point on proceeding otherwise
	waitForInfluxDB(&cfg.InfluxDB, logger)

	// application instance
	app := &appCfg{
		cfg:    cfg,
		logger: logger,
		dynamo: dynamo.New(cfg.Port, &cfg.Dynamo, &cfg.InfluxDB, logger),
	}
	// start Dynamo-related tasks (these run in the background and do not block)
	app.dynamo.Start()
	// listen and serve ChronosDB (most of the Dynamo-related tasks above )
	serve(app)
}

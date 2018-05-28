package main

import (
	"flag"
	"fmt"
	"math/rand"
	// _ "net/http/pprof"
	"os"
	"time"

	"github.com/marcoalmeida/chronosdb/config"
	"github.com/marcoalmeida/chronosdb/dynamo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type appCfg struct {
	cfg    *config.MainCfg
	logger *zap.Logger
	dynamo *dynamo.Dynamo
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	// logging
	atom := zap.NewAtomicLevel()
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.Lock(os.Stdout),
		atom,
	))
	// flush the buffer before exiting
	defer logger.Sync()

	var cfgFile = flag.String("cfg", "", "path to the configuration file")
	flag.Parse()

	// parse the main configuration file (will default to standard locations if no file was set using the flag above)
	cfg, err := config.New(cfgFile, logger)
	if err != nil {
		logger.Fatal("Configuration error:", zap.Error(err))
	}
	// having parsed the configuration file, set the correct log level
	if cfg.EnableDebug {
		atom.SetLevel(zap.DebugLevel)
	}

	logger.Debug("Application configuration", zap.String("options", fmt.Sprintf("%+v", cfg)))

	// application instance
	app := &appCfg{
		cfg:    cfg,
		logger: logger,
		dynamo: dynamo.New(cfg.Port, &cfg.Dynamo, &cfg.InfluxDB, logger),
	}

	// start the main app
	run(app)
}

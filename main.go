package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/marcoalmeida/chronosdb/chronos"
	"github.com/marcoalmeida/chronosdb/config"
	"github.com/marcoalmeida/chronosdb/handlers"
	"github.com/marcoalmeida/chronosdb/influxdb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type app struct {
	listen  string
	port    int64
	logger  *zap.Logger
	chronos *chronos.Chronos
}

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

func mustParseConfig(logger *zap.Logger) *config.ChronosCfg {
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
	app := &app{
		listen:  cfg.ListenIP,
		port:    cfg.Port,
		logger:  logger,
		chronos: chronos.New(cfg, logger),
	}

	// start ChronosDB-related tasks (these run in the background as to not block getting to listen-and-serve)
	app.chronos.Start()

	// listen and serve ChronosDB (most of the Chronos-related tasks above )
	serve(app)
}

// setup handlers, listen and serve ChronosDB
func serve(app *app) {
	env := &handlers.Env{
		Chronos: app.chronos,
		Logger:  app.logger,
	}
	handlers.Register(env)

	app.logger.Info(
		"Ready to listen",
		zap.Int64("port", app.port),
		zap.String("IP", app.listen),
	)

	listenOn := fmt.Sprintf("%s:%d", app.listen, app.port)
	err := http.ListenAndServe(listenOn, nil)
	if err != nil {
		app.logger.Error("Server error", zap.Error(err))
	}
}

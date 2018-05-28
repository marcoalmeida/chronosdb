package dynamo

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/marcoalmeida/chronosdb/responsetypes"
	"github.com/marcoalmeida/chronosdb/shared"
	"go.uber.org/zap"
)

func (dyn *Dynamo) initialize() {
	// always fetch all DBs from all other nodes and create them locally if they don't already exist
	// this should be done every time a node starts because we don't know whether it's a new member of the ring,
	// returning after a prolonged outage during which DBs were created, an attempt at merging clusters, ...
	// TODO: create users and access control rules

	dyn.logger.Debug("Initialization process: creating DBs")
	// don't go anywhere until all DBs have been synced
	// TODO: give up after a while -- otherwise we'll block forever if a single node is down
	for {
		dbs, err := dyn.fetchAllDBs()
		if err != nil {
			dyn.logger.Error("Failed to get remote list of databases", zap.Error(err))
			// sleep for a period of time and then repeat until we successfully get the list of existing DBs
			r := rand.Intn(dyn.cfg.HandoffInterval)
			time.Sleep(time.Duration(r) * time.Second)
			continue
		}
		dyn.logger.Debug("Received list of databases", zap.Strings("databases", dbs))
		err = dyn.createAllDBs(dbs)
		if err != nil {
			dyn.logger.Fatal(
				"Failed to create DB while bootstrapping",
				zap.Error(err),
			)
		}

		dyn.logger.Info("Initialization process completed")
		dyn.isInitializing = false
		break
	}
}

// fetch all DBs from all nodes in the ring
func (dyn *Dynamo) fetchAllDBs() ([]string, error) {
	allDBs := []string{}
	ring := dyn.GetRing()

	for _, node := range ring.Nodes {
		if node == dyn.cfg.NodeID {
			// the current node doesn't have the list of DBs, it *needs* it
			continue
		}

		dyn.logger.Debug("Fetching remote DBs", zap.String("node", node))
		status, response := shared.DoGet(
			fmt.Sprintf("http://%s:%d/db/", node, dyn.chronosDBPort),
			nil,
			dyn.httpClient,
			dyn.cfg.MaxRetries,
			dyn.logger,
			"dynamo.fetchAllDBs()",
		)
		if !(status >= 200 && status <= 299) {
			return nil, errors.New("failed to fetch list of databases from node " + node + ": " + string(response))
		}

		dbs := &responsetypes.GetDBs{}
		err := json.Unmarshal(response, &dbs)
		if err != nil {
			return nil, errors.New("failed to parse list of databases from node " + node + ": " + err.Error())
		}

		allDBs = append(allDBs, dbs.Databases...)
	}

	// we don't know for sure, but there might be DBs around that need to be replicated but we could not get them
	return allDBs, nil
}

func (dyn *Dynamo) createAllDBs(dbs []string) error {
	for _, db := range dbs {
		err := dyn.influxDB.CreateDB(db)
		if err != nil {
			return err
		}
	}

	return nil
}

package chronos

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/thumbtack/go/images/errors"
	"go.uber.org/zap"
)

func (d *Chronos) initialize() {
	// always fetch all DBs from all other nodes and create them locally if they don't already exist
	// this should be done every time a node starts because we don't know whether it's a new member of the cluster,
	// returning after a prolonged outage during which DBs were created, an attempt at merging clusters, ...
	// TODO: create users and access control rules

	d.isInitializing = true
	d.logger.Debug("Initialization process: creating DBs")
	// don't go anywhere until all DBs have been created
	for d.isInitializing {
		dbs, err := d.fetchAllDBs()
		if err != nil {
			d.logger.Error("Failed to get remote list of databases", zap.Error(err))
			// sleep for a period of time and then repeat until we successfully get the list of existing DBs
			r := rand.Intn(d.cfg.ReplayInterval)
			time.Sleep(time.Duration(r) * time.Second)
			continue
		}

		d.logger.Debug("Received list of databases", zap.Strings("databases", dbs))
		err = d.createDBs(dbs)
		if err != nil {
			d.logger.Fatal("Failed to create DB while initializing", zap.Error(err))
		}

		d.logger.Info("Initialization process completed")
		d.isInitializing = false
	}
}

// fetch all DBs from all nodes in the cluster
// all nodes are expected to have all DBs so return success if the read quorum is met
func (d *Chronos) fetchAllDBs() ([]string, error) {
	keyDelimiter := "_"
	// keep track of how many times a given list of DBs occurs
	dbsCount := make(map[string]int)
	cluster := d.GetCluster()

	for _, node := range cluster.Nodes {
		if node == d.cfg.NodeID {
			// the current node doesn't have the list of DBs, it *needs* it
			continue
		}

		d.logger.Debug("Fetching remote DBs", zap.String("node", node))
		dbs, err := d.gossip.GetDBs(node)
		if err != nil {
			d.logger.Error("Failed to fetch remote DBs", zap.String("node", node), zap.Error(err))
		} else {
			d.logger.Debug("Found remote DBs", zap.String("node", node), zap.Strings("dbs", dbs.Databases))
			// slices can't be indexed, so we use the string that results from sort+concatenate
			sort.Strings(dbs.Databases)
			key := strings.Join(dbs.Databases, keyDelimiter)

			_, ok := dbsCount[key]
			if ok {
				dbsCount[key]++
			} else {
				dbsCount[key] = 1
			}
		}
	}

	// check quorum
	for k, v := range dbsCount {
		if v >= d.cfg.ReadQuorum {
			// filter out empty strings
			dbs := []string{}
			for _, db := range strings.Split(k, keyDelimiter) {
				if db != "" {
					dbs = append(dbs, db)
				}
			}

			return dbs, nil
		}
	}

	return nil, errors.New(fmt.Sprintf("not enough quorum"))
}

func (d *Chronos) createDBs(dbs []string) error {
	for _, db := range dbs {
		err := d.influxDB.CreateDB(db)
		if err != nil {
			return err
		}
	}

	return nil
}

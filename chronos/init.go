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

func (c *Chronos) initialize() {
	// always fetch all DBs from all other nodes and create them locally if they don't already exist
	// this should be done every time a node starts because we don't know whether it's a new member of the cluster,
	// returning after a prolonged outage during which DBs were created, an attempt at merging clusters, ...
	// TODO: create users and access control rules

	c.initializing = true
	c.logger.Debug("Initialization process: creating DBs")
	// don't go anywhere until all DBs have been created
	for c.initializing {
		dbs, err := c.fetchRemoteDBs()
		if err != nil {
			c.logger.Error("Failed to get remote list of databases", zap.Error(err))
			// sleep for a period of time and then repeat until we successfully get the list of existing DBs
			r := rand.Intn(c.cfg.ReplayInterval)
			time.Sleep(time.Duration(r) * time.Second)
			continue
		}

		c.logger.Debug("Received list of databases", zap.Strings("databases", dbs))
		err = c.createDBs(dbs)
		if err != nil {
			c.logger.Fatal("Failed to create DB while initializing", zap.Error(err))
		}

		c.logger.Info("Initialization process completed")
		c.initializing = false
	}
}

// fetch all DBs from all nodes in the cluster
// all nodes are expected to have all DBs so return success if the read quorum is met
func (c *Chronos) fetchRemoteDBs() ([]string, error) {
	keyDelimiter := "_"
	// keep track of how many times a given list of DBs occurs
	dbsCount := make(map[string]int)
	cluster := c.GetCluster()

	for _, node := range cluster.Nodes {
		if node == c.cfg.NodeID {
			// the current node doesn't have the list of DBs, it *needs* it
			continue
		}

		c.logger.Debug("Fetching remote DBs", zap.String("node", node))
		dbs, err := c.gossip.GetDBs(node)
		if err != nil {
			c.logger.Error("Failed to fetch remote DBs", zap.String("node", node), zap.Error(err))
		} else {
			c.logger.Debug("Found remote DBs", zap.String("node", node), zap.Strings("dbs", dbs.Databases))
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
		if v >= c.cfg.ReadQuorum {
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

func (c *Chronos) createDBs(dbs []string) error {
	for _, db := range dbs {
		err := c.influxDB.CreateDB(db)
		if err != nil {
			return err
		}
	}

	return nil
}

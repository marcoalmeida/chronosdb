package chronos

//func (c *Chronos) fsmTransferCollectTargets() (map[string][]*coretypes.Key, error) {
//	dbs, err := c.influxDB.ShowDBs()
//	if err != nil {
//		c.logger.Error(
//			"Failed to list DBs",
//			zap.Error(err),
//		)
//		return nil, err
//	}
//
//	// find all keys (db, measurement) that this node keeps which are also relevant for other nodes
//	targets := map[string][]*coretypes.Key{}
//	for _, db := range dbs {
//		measurements, err := c.influxDB.ShowMeasurements(db)
//		if err != nil {
//			c.logger.Error(
//				"Failed to list measurements",
//				zap.Error(err),
//			)
//			return nil, err
//		}
//
//		for _, m := range measurements {
//			key := coretypes.NewKey(db, m)
//			nodes := c.cluster.GetNodesRanked(key.String())
//			if len(nodes) < c.cfg.NumberOfReplicas {
//				c.logger.Error(
//					"Not enough nodes available",
//					zap.Int("need", c.cfg.NumberOfReplicas),
//					zap.Int("found", len(nodes)),
//				)
//				return nil, errors.New("not enough replicas available")
//			}
//			// get the first N nodes responsible for this key
//			nodes = nodes[:c.cfg.NumberOfReplicas]
//			for _, n := range nodes {
//				// skip self
//				if n != c.cfg.NodeID {
//					targets[n] = append(targets[n], key)
//				}
//			}
//		}
//	}
//
//	return targets, nil
//}

//func (c *Chronos) fsmTransferAllKeys(node string, keys []*coretypes.Key) {
//	order := rand.Perm(len(keys))
//	for _, i := range order {
//		key := keys[i]
//
//		keyExists, err := c.gossip.AskHasKey(node, key)
//		if err != nil {
//			c.logger.Error(
//				"Failed to query node for key",
//				zap.Error(err),
//				zap.String("node", node),
//				zap.String("key", key.String()),
//			)
//			// nothing else we can do here; maybe the next round will succeed?
//			continue
//		}
//		if keyExists {
//			// no point on transferring it
//			c.logger.Debug(
//				"Skipping existing key",
//				zap.String("node", node),
//				zap.String("key", key.String()),
//			)
//		} else {
//			// transfer the key, move on to the next one
//			c.logger.Debug(
//				"Starting to transfer key",
//				zap.String("node", node),
//				zap.String("key", key.String()),
//			)
//			c.fsmTransferKey(node, key)
//		}
//	}
//}

//// read the local InfluxDB and generate hinted handoffs -- unlike a typical handoff though, the target node needs to
//// know when the transfer has completed, and both source and target must be able to resume from restarts
//func (c *Chronos) fsmTransferKey(node string, key *coretypes.Key) {
//	batch := 0
//	transferResultChan := make(chan fsmForwardWriteResult, 1)
//
//	for {
//		metrics, err := c.influxDB.QueryToLineProtocol(
//			key.DB,
//			key.Measurement,
//			c.cfg.CrossCheckBatchSize,
//			c.cfg.CrossCheckBatchSize*batch,
//		)
//		if err != nil {
//			// not much we can do; just make sure not to increment the counter so that we repeat the batch and carry on
//			c.logger.Error(
//				"Failed to query metrics",
//				zap.Error(err),
//				zap.String("node", node),
//				zap.String("key", key.String()),
//				zap.Error(err),
//			)
//			continue
//		}
//		if len(metrics) == 0 {
//			c.logger.Info(
//				"Key transfer completed",
//				zap.String("node", node),
//				zap.String("key", key.String()),
//			)
//			err := c.gossip.TellHasKey(node, key)
//			for err != nil {
//				c.logger.Error(
//					"Failed to notify node that transfer is completed",
//					zap.String("node", node),
//					zap.String("key", key.String()),
//					zap.Error(err),
//				)
//				// wait before retrying
//				time.Sleep(time.Duration(c.cfg.CrossCheckSendInterval) * time.Second)
//				err = c.gossip.TellHasKey(node, key)
//			}
//			return
//		}
//		c.fsmForwardWrite(node, c.createKeyTransferURL(key), key, metrics, transferResultChan)
//		result := <-transferResultChan
//		if result.httpStatus >= 200 && result.httpStatus <= 299 {
//			c.logger.Debug("Successfully transferred key batch",
//				zap.String("key", key.String()),
//				zap.Int("batch", batch),
//			)
//			// move on to the next batch
//			batch++
//		} else {
//			c.logger.Error("Failed to transfer key batch",
//				zap.String("key", key.String()),
//				zap.Int("batch", batch),
//			)
//		}
//	}
//}

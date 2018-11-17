package ilog

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"github.com/marcoalmeida/chronosdb/mapsort"
	"github.com/marcoalmeida/chronosdb/shared"
	"go.uber.org/zap"
)

// ChronosDB Intent Log
//
// The Intent Log is used by ChronosDB to deal with temporary node failures.
// When a given replica A is down, the payload intended to A is stored locally on the coordinator node. When the
// failed node is back online the payload is replayed and the local copy removed.

// directory structure and modus operandi for replaying entries
//
// this package manages local copies of
//
// in order to make sure the background tasks that replay entries in the IL will not see a partially written payload,
// the following method is implemented:
// - one directory per node, each containing two sub-directories: tmp and new
// - each entry is written to $chronosdb/intent_log/<node>/tmp/<unique>, where <unique> is a unique name
// - once successfully written, the file is moved to $chronosdb/intent_log/<node>/new/<unique> using rename (atomic [1])
//
// there can be multiple threads reading entries at the same time

// the format of a entry file: simple text file with metrics defined as per the line protocol plus the original URI
// (which we need to create the request):
//
// node
// URI
// Key
// payload
// ...
// payload
//
// [1] - http://pubs.opengroup.org/onlinepubs/9699919799/functions/rename.html

const (
	intentLogDirectoryName  = "intent_log"
	tmpDirectoryName        = "tmp"
	newDirectoryName        = "new"
	processingDirectoryName = "processing"
)

type Entry struct {
	Node string
	URI  string
	// keep db and measurement for easier access to these values (e.g., logging)
	Key     *coretypes.Key
	Payload []byte
	// full path to the entry itself so that we can Remove it after replayed
	filePath string
}

type ILog struct {
	dataDir         string
	replicaPriority map[string]int
	logger          *zap.Logger
}

func New(dataDir string, logger *zap.Logger) *ILog {
	return &ILog{
		dataDir: dataDir,
		// assign a priority to each replica for which there's an intent log entry (higher ==> lower priority)
		replicaPriority: make(map[string]int, 0),
		logger:          logger,
	}
}

func (i *ILog) intentLogRoot() string {
	return fmt.Sprintf("%s/%s", i.dataDir, intentLogDirectoryName)
}

func (i *ILog) getPath(node string, dir string) string {
	return filepath.Join(i.intentLogRoot(), node, dir)
}

func (i *ILog) tmpDir(node string) string {
	return i.getPath(node, tmpDirectoryName)
}

func (i *ILog) newDir(node string) string {
	return i.getPath(node, newDirectoryName)
}

func (i *ILog) processingDir(node string) string {
	return i.getPath(node, processingDirectoryName)
}

// Add writes a new entry to the intent log.
func (i *ILog) Add(entry *Entry) error {
	tmpDir := i.tmpDir(entry.Node)
	newDir := i.newDir(entry.Node)

	if err := shared.EnsureDirectory(tmpDir); err != nil {
		return err
	}

	if err := shared.EnsureDirectory(newDir); err != nil {
		return err
	}

	// TODO: use time.Now().UnixNano() as the file name to simplify sorting by timestamp
	fTmp, err := ioutil.TempFile(tmpDir, "")
	if err != nil {
		return err
	}

	// create a compressed file
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	// headers
	zw.ModTime = time.Now()
	// entry data
	zw.Write([]byte(entry.Node))
	zw.Write([]byte("\n"))
	zw.Write([]byte(entry.URI))
	zw.Write([]byte("\n"))
	zw.Write([]byte(entry.Key.String()))
	zw.Write([]byte("\n"))
	zw.Write(entry.Payload)
	// make sure to close the writer to flush the buffer before writing the file
	zw.Close()

	// write the buffer to the temporary file
	fTmp.Write(buf.Bytes())
	fTmp.Close()

	// move from tmp -> new
	fNew, err := ioutil.TempFile(newDir, "")
	if err != nil {
		return err
	}
	fNew.Close()

	if err := os.Rename(fTmp.Name(), fNew.Name()); err != nil {
		return err
	}

	return nil
}

// TODO: sort by time stamp and deliver older entries first
// Fetch finds and returns an intent log entry (oldest one first) that should be replayed
func (i *ILog) Fetch() (*Entry, error) {
	// list dirs -- each one is a node
	dirs, err := ioutil.ReadDir(i.intentLogRoot())
	if err != nil {
		return nil, err
	}

	// make sure every node has a priority assigned
	for _, d := range dirs {
		node := d.Name()
		if _, ok := i.replicaPriority[node]; !ok {
			i.replicaPriority[node] = 0
		}
	}

	// sort the replicas by ascending priority order
	for _, node := range mapsort.ByValue(i.replicaPriority, true) {
		path := i.newDir(node)
		entries, err := ioutil.ReadDir(path)
		if err != nil {
			return nil, err
		}

		if len(entries) > 0 {
			f := entries[0]
			// move to "processing"
			processingDir := i.processingDir(node)
			if err := shared.EnsureDirectory(processingDir); err != nil {
				return nil, err
			}
			fProcessing, err := ioutil.TempFile(processingDir, "")
			if err != nil {
				return nil, err
			}
			fProcessing.Close()

			if err := os.Rename(path+"/"+f.Name(), fProcessing.Name()); err != nil {
				return &Entry{}, err
			}

			// read, split lines, and return the appropriate struct
			file, err := os.Open(fProcessing.Name())
			if err != nil {
				return nil, err
			}

			zr, err := gzip.NewReader(file)
			if err != nil {
				return nil, err
			}

			data, err := ioutil.ReadAll(zr)
			if err != nil {
				return nil, err
			}

			file.Close()
			zr.Close()

			lines := bytes.Split(data, []byte("\n"))
			return &Entry{
				Node:     string(lines[0]),
				URI:      string(lines[1]),
				Key:      coretypes.KeyFromString(string(lines[2])),
				Payload:  bytes.Join(lines[3:], []byte("\n")),
				filePath: fProcessing.Name(),
			}, nil
		}
	}

	// if we made it this far, there are no entries to replay
	return nil, nil
}

// Remove deletes a given entry from the local file system
func (i *ILog) Remove(contents *Entry) error {
	i.logger.Debug("Removing already replayed entry", zap.String("file", contents.filePath))
	return os.Remove(contents.filePath)
}

// RemoveStale removes entries intended to nodes that are no longer part of the cluster
func (i *ILog) RemoveStale(cluster []string) {
	i.logger.Info("Starting the removal of stale intent log entries...")

	targets, err := ioutil.ReadDir(i.intentLogRoot())
	if err != nil {
		i.logger.Error("Failed to open the intent log", zap.Error(err))
		return
	}

	for _, t := range targets {
		exists := false
		for _, n := range cluster {
			if t.Name() == n {
				exists = true
				break
			}
		}
		if !exists {
			path := filepath.Join(i.intentLogRoot(), t.Name())
			i.logger.Info(
				"Removing entry",
				zap.String("node", t.Name()),
				zap.String("path", path),
			)
			// actually delete the file
			if err := os.Remove(path); err != nil {
				i.logger.Error(
					"Failed to Remove entry",
					zap.String("node", t.Name()),
					zap.String("path", path),
					zap.Error(err),
				)
			}
		}
	}

	i.logger.Info("Finished the removal of stale intent log entries...")
}

// RestoreDangling finds and restores back to the log entries that were in the process of being replayed, but for
// some reason the process was interrupted before successfully completed.
func (i *ILog) RestoreDangling() {
	// move dangling files from the `processing` directory back to `new` for re-processing
	i.logger.Info("Starting recovery of dangling intent log entries...")

	nodes, err := ioutil.ReadDir(i.intentLogRoot())
	if err != nil {
		i.logger.Error("Failed to read the intent log directory", zap.Error(err))
		return
	}

	for _, node := range nodes {
		// there may not be a processing directory, in which case there's nothing to try to recover
		if _, err := os.Stat(i.processingDir(node.Name())); os.IsNotExist(err) {
			continue
		}

		entries, err := ioutil.ReadDir(i.processingDir(node.Name()))
		if err != nil {
			i.logger.Error("Failed to list intent log entries", zap.Error(err))
			return
		}

		for _, entry := range entries {
			src := i.processingDir(node.Name()) + "/" + entry.Name()
			dst := i.newDir(node.Name()) + "/" + entry.Name()
			i.logger.Info(
				"Moving dangling intent log entry",
				zap.String("src", src),
				zap.String("dst", dst))
			err := os.Rename(src, dst)
			if err != nil {
				i.logger.Error("Failed to move intent log entry", zap.Error(err))
			}
		}
	}

	i.logger.Info("Finished recovery of dangling intent log entries...")
}

// ReAdd changes the state of a log entry from being processed to available for processing
func (i *ILog) ReAdd(entry *Entry) {
	// lower the priority of the node to which this entry was intended (or initialize it)
	if _, ok := i.replicaPriority[entry.Node]; ok {
		i.replicaPriority[entry.Node]++
	} else {
		i.replicaPriority[entry.Node] = 0
	}
	i.logger.Debug(
		"Updated intent log priority",
		zap.String("node", entry.Node),
		zap.Int("priority", i.replicaPriority[entry.Node]),
	)

	// move the entry from `processing` to `new`
	src := entry.filePath
	splitPath := strings.Split(src, "/")
	entryName := splitPath[len(splitPath)-1]
	// the name is generated using nanoseconds from Epoch and this one in particular will be in the past so it's
	// impossible to have a collision and this operation is safe
	dst := i.newDir(entry.Node) + "/" + entryName

	err := os.Rename(src, dst)
	if err != nil {
		i.logger.Error("Failed to reset chunk", zap.Error(err))
	}
}

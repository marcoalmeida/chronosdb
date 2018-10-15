package chunk

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
	"go.uber.org/zap"
)

// directory structure and modus operandi for replaying chunks
//
// Chunk replay is how ChronosDB deals with temporary node failures.
// When a given replica A is down the payload intended to A is stored locally on the coordinator node. When the
// failed node returns to the cluster, the
// updates received by the neighboring nodes are handed off to it.

// this package manages local copies of
//
// chunks of metrics that failed to be forwarded to a replica by keeping a local copy
//
// in order to make sure the background tasks that replay chunks will not see a partially written payload, the
// following method is implemented:
// - one directory per node, each containing two sub-directories: tmp and new
// - each chunk is written to $chronosdb/chunks/<node>/tmp/<unique>, where <unique> is a unique name
// - once successfully written, the file is moved to $chronosdb/chunks/<node>/new/<unique> using rename (atomic [1])
//
// there can be multiple threads reading chunks at the same time

// the format of a chunk file: simple text file with metrics defined as per the line protocol plus the original URI
// (which we need to create the request):
//
// node
// URI
// DB
// Measurement
// payload
// ...
// payload
//
// [1] - http://pubs.opengroup.org/onlinepubs/9699919799/functions/rename.html

const (
	chunksDirectoryName      = "chunks"
	tmpDirectoryName        = "tmp"
	newDirectoryName        = "new"
	processingDirectoryName = "processing"
)

type Hint struct {
	Node string
	URI  string
	// keep db and measurement for easier access to these values (e.g., logging)
	Key      *coretypes.Key
	Payload  []byte
	filePath string
}

type Cfg struct {
	dataDir string
	logger  *zap.Logger
}

func New(dataDir string, logger *zap.Logger) *Cfg {
	return &Cfg{
		dataDir: dataDir,
		logger:  logger,
	}
}

func (h *Cfg) getPath(node string, dir string) string {
	return filepath.Join(h.chunksRoot(), node, dir)
}

func (h *Cfg) chunksRoot() string {
	return fmt.Sprintf("%s/%s", h.dataDir, chunksDirectoryName)
}

func (h *Cfg) tmpDir(node string) string {
	return h.getPath(node, tmpDirectoryName)
}

func (h *Cfg) newDir(node string) string {
	return h.getPath(node, newDirectoryName)
}

func (h *Cfg) processingDir(node string) string {
	return h.getPath(node, processingDirectoryName)
}

// Store saves a chunk in the local file system. A background process will hand it off once the target node is available.
func (h *Cfg) Store(chunk *Hint) error {
	tmpDir := h.tmpDir(chunk.Node)
	newDir := h.newDir(chunk.Node)

	if err := ensureDirectory(tmpDir); err != nil {
		return err
	}

	if err := ensureDirectory(newDir); err != nil {
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
	// chunk data
	zw.Write([]byte(chunk.Node))
	zw.Write([]byte("\n"))
	zw.Write([]byte(chunk.URI))
	zw.Write([]byte("\n"))
	zw.Write([]byte(chunk.Key.String()))
	zw.Write([]byte("\n"))
	zw.Write(chunk.Payload)
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

// TODO: sort by time stamp and deliver older chunks first
// Fetch finds and returns a (any) saved chunk that should be handed off
func (h *Cfg) Fetch() (*Hint, error) {
	// list nodes
	nodes, err := ioutil.ReadDir(h.chunksRoot())
	if err != nil {
		return nil, err
	}

	for _, n := range nodes {
		path := h.newDir(n.Name())
		chunks, err := ioutil.ReadDir(path)
		if err != nil {
			return nil, err
		}

		if len(chunks) > 0 {
			f := chunks[0]
			// move to "processing"
			processingDir := h.processingDir(n.Name())
			if err := ensureDirectory(processingDir); err != nil {
				return nil, err
			}
			fProcessing, err := ioutil.TempFile(processingDir, "")
			if err != nil {
				return nil, err
			}
			fProcessing.Close()

			if err := os.Rename(path+"/"+f.Name(), fProcessing.Name()); err != nil {
				return &Hint{}, err
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
			return &Hint{
				Node:     string(lines[0]),
				URI:      string(lines[1]),
				Key:      coretypes.KeyFromString(string(lines[2])),
				Payload:  bytes.Join(lines[3:], []byte("\n")),
				filePath: fProcessing.Name(),
			}, nil
		}
	}

	// if we made it this far, there are no chunks to replay
	return nil, nil
}

// Remove deletes a given chunk from the local file system
func (h *Cfg) Remove(contents *Hint) error {
	h.logger.Debug("Removing already handed off chunk", zap.String("file", contents.filePath))
	return os.Remove(contents.filePath)
}

// RemoveStale removes chunks intended to nodes that are no longer active
func (h *Cfg) RemoveStale(activeNodes []string) {
	h.logger.Info("Starting the removal of stale chunks...")

	targets, err := ioutil.ReadDir(h.chunksRoot())
	if err != nil {
		h.logger.Error("Failed to list the root directory for chunks", zap.Error(err))
		return
	}

	for _, t := range targets {
		exists := false
		for _, n := range activeNodes {
			if t.Name() == n {
				exists = true
				break
			}
		}
		if !exists {
			path := filepath.Join(h.chunksRoot(), t.Name())
			h.logger.Info(
				"Removing chunk",
				zap.String("node", t.Name()),
				zap.String("path", path),
			)
			// actually delete the file
			if err := os.Remove(path); err != nil {
				h.logger.Error(
					"Failed to remove chunk",
					zap.String("node", t.Name()),
					zap.String("path", path),
					zap.Error(err),
				)
			}
		}
	}

	h.logger.Info("Finished the removal of stale chunks...")
}

// RestoreDangling moves dangling files from the `processing` state back to `new` for re-processing. Dangling chunks
// will exist if the hand off process was interrupted
func (h *Cfg) RestoreDangling() {
	h.logger.Info("Starting recovery of dangling chunks...")

	nodes, err := ioutil.ReadDir(h.chunksRoot())
	if err != nil {
		h.logger.Error("Failed to list chunks root directory", zap.Error(err))
		return
	}

	for _, node := range nodes {
		// there may not be a processing directory, in which case there's nothing to try to recover
		if _, err := os.Stat(h.processingDir(node.Name())); os.IsNotExist(err) {
			continue
		}

		chunks, err := ioutil.ReadDir(h.processingDir(node.Name()))
		if err != nil {
			h.logger.Error("Failed to list chunks", zap.Error(err))
			return
		}

		for _, chunk := range chunks {
			src := h.processingDir(node.Name()) + "/" + chunk.Name()
			dst := h.newDir(node.Name()) + "/" + chunk.Name()
			h.logger.Info(
				"Moving dangling chunk",
				zap.String("src", src),
				zap.String("dst", dst))
			err := os.Rename(src, dst)
			if err != nil {
				h.logger.Error("Failed to move chunk", zap.Error(err))
			}
		}
	}

	h.logger.Info("Finished recovery of dangling chunks...")
}

// SaveForRerun moves a chunk from the `processing` state back to `new`
func (h *Cfg) SaveForRerun(chunk *Hint) {
	src := chunk.filePath
	splitPath := strings.Split(src, "/")
	chunkName := splitPath[len(splitPath)-1]
	// the name is generated using nanoseconds from Epoch and this one in particular will be in the past so it's
	// impossible to have a collision and this operation is safe
	dst := h.newDir(chunk.Node) + "/" + chunkName

	err := os.Rename(src, dst)
	if err != nil {
		h.logger.Error("Failed to reset chunk", zap.Error(err))
	}
}

// create a directory structure if it does not yet exist (or return an error trying)
func ensureDirectory(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.MkdirAll(path, 0755)
		if err != nil {
			return err
		}
	}

	return nil
}

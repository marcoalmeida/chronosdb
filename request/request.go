// Package request provides a number of helper functions to manipulate requests to ChronosDB.
// Some examples include forming URLs, setting the appropriate headers, checking for the presence
// of a given header, etc.
package request

import (
	"fmt"
	"net/http"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"go.uber.org/zap"
)

type Request struct {
	chronosDBPort int64
	logger        *zap.Logger
}

// headers used by ChronosDB to indicate the type/source of a request and extend with extra information
const (
	headerXForward    = "X-ChronosDB-Forward"
	headerXKey        = "X-ChronosDB-Key"
	headerXCrosscheck = "X-ChronosDB-Crosscheck"
	headerXIntentLog  = "X-ChronosDB-IntentLog"
)

// New returns an instance of Request
func New(chronosDBPort int64, logger *zap.Logger) *Request {
	return &Request{
		logger:        logger,
		chronosDBPort: chronosDBPort,
	}
}

// GenerateForwardURL generates a URL to be used for forwarding a request to a ChronosDB node.
func (r *Request) GenerateForwardURL(node string, uri string) string {
	if node == "" {
		return ""
	}

	// make the root endpoint explicit
	if uri == "" {
		uri = "/"
	}

	u := fmt.Sprintf("http://%s:%d%s", node, r.chronosDBPort, uri)
	r.logger.Debug("Generating forward URL", zap.String("url", u))
	return u
}

// SetForwardHeaders marks a request (by adding a header) as being forwarded from a coordinator to its final
// destination. Some forwarded requests (read and write metrics, for example) include a key which is also added
// to a header. Requests that do not include it (like creating a DB) should set key to nil.
func (r *Request) SetForwardHeaders(key *coretypes.Key, headers *http.Header) {
	if key != nil {
		headers.Set(headerXKey, key.String())
	}

	headers.Set(headerXForward, "true")
}

// NodeIsCoordinator returns true iff the request is not being forwarded from another ChronosDB instance and the
// node receiving it should coordinate its execution.
func (r *Request) NodeIsCoordinator(headers http.Header) bool {
	return headers.Get(headerXForward) != "true"
}

// SetCrosscheckHeaders marks a request as originating from the cross-check process. A key is always expected
// and the corresponding header is set to whatever the Stringer implementation returns.
func (r *Request) SetCrosscheckHeaders(key *coretypes.Key, headers *http.Header) {
	if key == nil {
		r.logger.Error("Found nil key while trying to set cross-check headers")
	}
	headers.Set(headerXKey, key.String())
	headers.Set(headerXCrosscheck, "true")
}

// RequestIsCrosscheck returns true iff the current request is part of the cross-check process.
func (r *Request) RequestIsCrosscheck(headers http.Header) bool {
	return headers.Get(headerXCrosscheck) == "true"
}

// mark a request as originating from replaying an intent log
func (r *Request) SetIntentLogHeaders(key *coretypes.Key, headers *http.Header) {
	if key == nil {
		r.logger.Error("Found nil key while setting intent log headers")
	}
	headers.Set(headerXKey, key.String())
	headers.Set(headerXIntentLog, "true")
}

// return true iff the current request is being replayed from and intent log
func (r *Request) RequestIsIntentLog(headers http.Header) bool {
	return headers.Get(headerXIntentLog) == ""
}

// GetKeyFromHeader extracts the key name from a request's header and returns a *Key instance
func (r *Request) GetKeyFromHeader(headers http.Header) *coretypes.Key {
	return coretypes.KeyFromString(headers.Get(headerXKey))
}

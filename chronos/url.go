// TODO: this can probably be moved to its own package
//  (maybe `requests` as this is more than just URLs?) -- only one dependency on ChronosDB
//  (which can be easily injected) and this package is very self-contained

package chronos

import (
	"fmt"
	"net/http"

	"github.com/marcoalmeida/chronosdb/coretypes"
)

// query string parameters used by ChronosDB (appended to whatever InfluxDB uses for internal use)
const (
	headerXForward    = "X-ChronosDB-Forward"
	headerXKey        = "X-ChronosDB-Key"
	headerXCrosscheck = "X-ChronosDB-Crosscheck"
	headerXIntentLog  = "X-ChronosDB-IntentLog"
)

// generate a URL to be used for forwarding a request
func (c *Chronos) generateForwardURL(node string, uri string) string {
	return fmt.Sprintf("http://%s:%d%s", node, c.cfg.Port, uri)
}

// mark a request as being forwarded from a coordinator to its final destination
// some forwarded requests include a key; others, like create DB, do not
func setForwardHeaders(key *coretypes.Key, headers *http.Header) {
	if key != nil {
		headers.Set(headerXKey, key.String())
	}

	headers.Set(headerXForward, "true")
}

// return true iff the current node is coordinating this request, i.e., the `forward` header has not been set
func nodeIsCoordinator(headers http.Header) bool {
	return headers.Get(headerXForward) != "true"
}

// mark a request as originating from the cross-check process
func (c *Chronos) setCrosscheckHeaders(key *coretypes.Key, headers *http.Header) {
	if key == nil {
		c.logger.Error("Found nil key while setting cross-check headers")
	}
	headers.Set(headerXKey, key.String())
	headers.Set(headerXCrosscheck, "true")
}

// return true iff the current request is part of the cross-check process, i.e.,
// the `crosscheck` header has been set
func (c *Chronos) requestIsCrosscheck(headers http.Header) bool {
	return headers.Get(headerXCrosscheck) == ""
}

func (c *Chronos) setIntentLogHeaders(key *coretypes.Key, headers *http.Header) {
	if key == nil {
		c.logger.Error("Found nil key while setting intent log headers")
	}
	headers.Set(headerXKey, key.String())
	headers.Set(headerXIntentLog, "true")
}

// return true iff the current request is part of the cross-check process, i.e.,
// the `crosscheck` header has been set
func (c *Chronos) requestIsIntentLog(headers http.Header) bool {
	return headers.Get(headerXIntentLog) == ""
}

func getKeyFromRequest(headers http.Header) *coretypes.Key {
	return coretypes.KeyFromString(headers.Get(headerXKey))
}

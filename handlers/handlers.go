package handlers

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/marcoalmeida/chronosdb/chronos"
	"github.com/marcoalmeida/chronosdb/coretypes"
	"github.com/marcoalmeida/chronosdb/responsetypes"
	"go.uber.org/zap"
)

// Env represents the basic environment on which we're service traffic
type Env struct {
	Chronos *chronos.Chronos
	Logger  *zap.Logger
}

// Response contains the necessary data to send an HTTP response back to the client. It may, or may not,
// be an interface that needs to be JSON-serialized before sending.
type Response struct {
	status   int
	data     []byte
	jsonData interface{}
}

// Handler is used to set up
type Handler struct {
	Env         *Env
	handlerFunc func(e *Env, r *http.Request) *Response
}

// ServeHTTP implements http.Handler and sends the actual response back to the client.
func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var err error

	// run the handler and get the response to be sent to the client
	resp := h.handlerFunc(h.Env, r)
	// start by sending the HTTP status code
	w.WriteHeader(resp.status)
	// the payload to send may be in the form of a simple slice of bytes or
	// an interface to be encoded and returned as JSON
	if resp.jsonData == nil {
		// send the sequence of bytes
		_, err = io.WriteString(w, string(resp.data))
	} else {
		// (try to) parse the JSON data and send the response
		err = json.NewEncoder(w).Encode(resp.jsonData)
	}

	// all we can do is log the error
	if err != nil {
		h.Env.Logger.Error("Failed to send response", zap.Error(err))
	}
}

// auxiliary functions to respond with common errors
func internalError(err error) *Response {
	return &Response{
		status:   http.StatusInternalServerError,
		data:     nil,
		jsonData: responsetypes.Error{Message: err.Error()},
	}
}

func unknownMethod() *Response {
	return &Response{
		status:   http.StatusBadRequest,
		data:     nil,
		jsonData: responsetypes.Error{Message: "unknown method"},
	}
}

// Register registers all handlers
func Register(env *Env) {
	// ChronosDB endpoints
	http.Handle("/status", Handler{Env: env, handlerFunc: status})
	http.Handle("/ring/", Handler{Env: env, handlerFunc: ring})
	http.Handle("/db/", Handler{Env: env, handlerFunc: db})
	http.Handle("/key/", Handler{Env: env, handlerFunc: key})
	// InfluxDB core endpoints
	http.Handle("/write", Handler{Env: env, handlerFunc: write})
	http.Handle("/query", Handler{Env: env, handlerFunc: query})
	// InfluxDB endpoints for the Prometheus remote read and write API (v1)
	http.Handle("/api/v1/prom/read", Handler{Env: env, handlerFunc: prometheusReadV1})
	http.Handle("/api/v1/prom/write", Handler{Env: env, handlerFunc: prometheusWriteV1})
	http.Handle("/api/v1/prom/metrics", Handler{Env: env, handlerFunc: prometheusMetricsV1})
}

//
// ChronosDB endpoints
//

func status(env *Env, r *http.Request) *Response {
	status := env.Chronos.NodeStatus()
	return &Response{
		status:   http.StatusOK,
		data:     nil,
		jsonData: status,
	}
}

func ring(env *Env, r *http.Request) *Response {
	// TODO: implement GET /ring/status to collect and return the status of all nodes
	switch r.Method {
	case "GET":
		nodes := env.Chronos.GetCluster()
		return &Response{
			status:   http.StatusOK,
			data:     nil,
			jsonData: nodes,
		}
	default:
		return unknownMethod()
	}
}

func db(env *Env, r *http.Request) *Response {
	err := r.ParseForm()
	if err != nil {
		return internalError(err)
	}

	// get the db name from the URL path
	db := r.URL.Path[len("/db/"):]

	switch r.Method {
	case "GET":
		dbs, err := env.Chronos.GetDBs()
		if err != nil {
			return internalError(err)
		} else {
			return &Response{status: http.StatusOK, data: nil, jsonData: dbs}
		}
	case "PUT", "DELETE":
		var node string
		var err error

		if r.Method == "PUT" {
			node, err = env.Chronos.CreateDB(r.Header, r.URL.RequestURI(), r.Form, db)
		} else {
			node, err = env.Chronos.DropDB(r.Header, r.URL.RequestURI(), r.Form, db)
		}
		if err != nil {
			return &Response{
				status:   http.StatusInternalServerError,
				data:     nil,
				jsonData: responsetypes.Error{Message: err.Error(), Node: node},
			}
		} else {
			return &Response{status: http.StatusOK, data: nil, jsonData: responsetypes.OK{Result: db}}
		}
	default:
		return unknownMethod()
	}
}

func key(env *Env, r *http.Request) *Response {
	k := r.URL.Path[len("/key/"):]
	if k == "" {
		return &Response{
			status:   http.StatusBadRequest,
			data:     nil,
			jsonData: responsetypes.Error{Message: "key not found"},
		}
	}

	key := coretypes.KeyFromString(k)
	switch r.Method {
	case "GET":
		keyStatus, err := env.Chronos.GetKeyStatus(key)
		if err != nil {
			return internalError(err)
		}

		return &Response{
			status:   http.StatusOK,
			data:     nil,
			jsonData: keyStatus,
		}
	case "PUT":
		// mark key as successfully transferred
		err := env.Chronos.KeyRecvCompleted(key)
		if err != nil {
			return internalError(err)
		} else {
			return &Response{status: http.StatusOK, data: nil, jsonData: responsetypes.OK{Result: "ok"}}
		}
	default:
		return unknownMethod()
	}
}

//
// InfluxDB core endpoints
//

// the /write and /query endpoints should be 100% compatible with InfluxDB
func write(env *Env, r *http.Request) *Response {
	// As per the documentation "The InfluxDB API makes no attempt to be RESTful."
	// and it shows; the response seems to usually be JSON, so we'll try to follow that here while dealing with
	// errors that must be dealt with before forwarding a request

	if r.Method != "POST" {
		return unknownMethod()
	}

	defer r.Body.Close()
	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		env.Logger.Error("Failed to read request body", zap.Error(err))
		return &Response{
			status:   http.StatusBadRequest,
			data:     nil,
			jsonData: responsetypes.Error{Message: err.Error()},
		}
	} else {
		status, response := env.Chronos.Write(r.Header, r.URL.RequestURI(), r.URL.Query(), payload)
		return &Response{
			status:   status,
			data:     response,
			jsonData: nil,
		}
	}
}

func query(env *Env, r *http.Request) *Response {
	// InfluxDB allows for both GET and POST requests when querying data, and the same parameters either as URL
	// parameters or as part of the body
	//
	// make sure we accept only these 2 methods, and simplify by calling ParseForm() and parsing the list of
	// queries from request.Form (which contains the parsed form data from both the URL and the POST or PUT form
	// data

	if r.Method != "POST" && r.Method != "GET" {
		return unknownMethod()
	}

	err := r.ParseForm()
	if err != nil {
		return &Response{
			status:   http.StatusBadRequest,
			data:     nil,
			jsonData: responsetypes.Error{Message: err.Error()},
		}
	}

	status, response := env.Chronos.Query(r.Header, r.URL.RequestURI(), r.Form)
	return &Response{
		status:   status,
		data:     response,
		jsonData: nil,
	}
}

//
// InfluxDB endpoints for the Prometheus remote read and write API (v1)
//

func prometheusReadV1(env *Env, r *http.Request) *Response {
	return nil
}

func prometheusWriteV1(env *Env, r *http.Request) *Response {
	return nil
}

func prometheusMetricsV1(env *Env, r *http.Request) *Response {
	return nil
}

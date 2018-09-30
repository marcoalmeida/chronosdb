package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/marcoalmeida/chronosdb/coretypes"
	"github.com/marcoalmeida/chronosdb/responsetypes"
	"go.uber.org/zap"
)

// Setup handlers, listen and serve ChronosDB
func serve(app *appCfg) {
	// compatible with InfluxDB
	//"/write"
	//"/query"
	// status | health -> number of nodes, key transfer, handoffs, ...

	// ChronosDB endpoints
	http.HandleFunc("/ring/", ringHandler(app))
	http.HandleFunc("/db/", dbHandler(app))
	http.HandleFunc("/key/", keyHandler(app))
	http.HandleFunc("/status", statusHandler(app))
	// InfluxDB core endpoints
	http.HandleFunc("/write", writeHandler(app))
	http.HandleFunc("/query", queryHandler(app))
	// InfluxDB endpoints for the Prometheus remote read and write API
	http.HandleFunc("/api/v1/prom/read", prometheusReadV1(app))
	http.HandleFunc("/api/v1/prom/write", prometheusWriteV1(app))
	http.HandleFunc("/api/v1/prom/metrics", prometheusMetricsV1(app))

	app.logger.Info(
		"Ready and listening",
		zap.Int64("port", app.cfg.Port),
		zap.String("IP", app.cfg.ListenIP))

	listenOn := fmt.Sprintf("%s:%d", app.cfg.ListenIP, app.cfg.Port)
	err := http.ListenAndServe(listenOn, nil)
	if err != nil {
		app.logger.Error("Server error", zap.Error(err))
	}
}

//
// ChronosDB endpoints
//

func statusHandler(app *appCfg) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := app.dynamo.NodeStatus()
		sendResponseJSON(w, status, http.StatusOK)
	}
}

func ringHandler(app *appCfg) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// TODO: implement GET /ring/status to collect and return the status of all nodes
		switch r.Method {
		case "GET":
			nodes := app.dynamo.GetRing()
			sendResponseJSON(w, nodes, http.StatusOK)
		default:
			sendResponseJSON(w, responsetypes.Error{Message: "Unknown method"}, http.StatusBadRequest)
		}
	}
}

func dbHandler(app *appCfg) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := r.ParseForm()
		if err != nil {
			sendResponseJSON(w, responsetypes.Error{Message: err.Error()}, http.StatusInternalServerError)
			return
		}

		// get the DB name from the URL path
		db := r.URL.Path[len("/db/"):]

		switch r.Method {
		case "GET":
			dbs, err := app.dynamo.GetDBs()
			if err != nil {
				sendResponseJSON(w, responsetypes.Error{Message: err.Error()}, http.StatusInternalServerError)
			} else {
				sendResponseJSON(w, dbs, http.StatusOK)
			}
		case "PUT", "DELETE":
			var node string
			var err error

			if r.Method == "PUT" {
				node, err = app.dynamo.CreateDB(r.URL.RequestURI(), r.Form, db)
			} else {
				node, err = app.dynamo.DropDB(r.URL.RequestURI(), r.Form, db)
			}
			if err != nil {
				response := responsetypes.Error{
					Message: err.Error(),
					Node:    node,
				}
				sendResponseJSON(w, response, http.StatusInternalServerError)
			} else {
				sendResponseJSON(w, responsetypes.OK{Result: db}, http.StatusOK)
			}

		default:
			sendResponseJSON(w, responsetypes.Error{Message: "unknown method"}, http.StatusBadRequest)
		}
	}
}

func keyHandler(app *appCfg) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		k := r.URL.Path[len("/key/"):]
		if k == "" {
			response := responsetypes.Error{Message: "key not found"}
			sendResponseJSON(w, response, http.StatusBadRequest)
			return
		}

		key := coretypes.KeyFromString(k)
		switch r.Method {
		case "GET":
			keyExists, err := app.dynamo.DoesKeyExist(key)
			if err != nil {
				sendResponseJSON(w, responsetypes.Error{Message: err.Error()}, http.StatusInternalServerError)
				return
			}

			if keyExists {
				sendResponseJSON(w, responsetypes.OK{Result: "ok"}, http.StatusOK)
			} else {
				sendResponseJSON(w, responsetypes.Error{Message: "key not found"}, http.StatusNotFound)
			}
		case "PUT":
			// mark key as successfully transferred
			err := app.dynamo.KeyRecvCompleted(key)
			if err != nil {
				sendResponseJSON(w, responsetypes.Error{Message: err.Error()}, http.StatusInternalServerError)
			} else {
				sendResponseJSON(w, responsetypes.OK{Result: "ok"}, http.StatusOK)
			}
		default:
			sendResponseJSON(w, responsetypes.Error{Message: "Unknown method"}, http.StatusBadRequest)
		}
	}
}

//
// InfluxDB core endpoints
//

// the /write and /query endpoints should be 100% compatible with InfluxDB
//
// extends InfluxDB's /write API by accepting an extra parameter, forward=false, to
// indicate the node
// should accept the payload instead of acting like a coordinator
//
// when present, this parameter instructs the Dynamo to call InfluxDB and store the payload locally
// if absent, the node handling it acts as a coordinator, finds all nodes where the data should be written to, and
// forwards it
//
// this endpoint should otherwise be compatible with InfluxDB's /write
func writeHandler(app *appCfg) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// As per the documentation "The InfluxDB API makes no attempt to be RESTful."
		// and it shows; the response seems to usually be JSON, so we'll try to follow that here while dealing with
		// errors that must be dealt with before forwarding a request

		if r.Method != "POST" {
			sendResponseJSON(w, responsetypes.Error{Message: "Unknown method"}, http.StatusBadRequest)
			return
		}

		defer r.Body.Close()
		payload, err := ioutil.ReadAll(r.Body)
		if err != nil {
			app.logger.Error("Failed to read request body", zap.Error(err))
			sendResponseJSON(w, responsetypes.Error{Message: err.Error()}, http.StatusBadRequest)
		} else {
			status, response := app.dynamo.Write(r.URL.RequestURI(), r.URL.Query(), payload)
			sendResponsePassThrough(w, response, status)
		}
	}
}

func queryHandler(app *appCfg) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// InfluxDB allows for both GET and POST requests when querying data, and the same parameters either as URL
		// parameters or as part of the body
		//
		// make sure we accept only these 2 methods, and simplify by calling ParseForm() and parsing the list of
		// queries from request.Form (which contains the parsed form data from both the URL and the POST or PUT form
		// data

		if r.Method != "POST" && r.Method != "GET" {
			sendResponseJSON(w, responsetypes.Error{Message: "Unknown method"}, http.StatusBadRequest)
			return
		}

		err := r.ParseForm()
		if err != nil {
			sendResponseJSON(w, responsetypes.Error{Message: err.Error()}, http.StatusBadRequest)
			return
		}

		status, response := app.dynamo.Query(r.URL.RequestURI(), r.Form)
		sendResponsePassThrough(w, response, status)
	}
}

//
// InfluxDB endpoints for the Prometheus remote read and write API (v1)
//

func prometheusReadV1(app *appCfg) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
	}
}

func prometheusWriteV1(app *appCfg) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
	}
}

func prometheusMetricsV1(app *appCfg) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
	}
}

//
// Helper methods for sending responses
//

func sendResponsePassThrough(w http.ResponseWriter, data []byte, status int) {
	w.WriteHeader(status)
	io.WriteString(w, string(data))
}

func sendResponseJSON(w http.ResponseWriter, data interface{}, status int) error {
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		// this should never, ever happen; still...
		return err
	}

	return nil
}

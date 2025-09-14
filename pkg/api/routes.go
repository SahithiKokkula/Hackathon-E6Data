package api

import (
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

type JSON map[string]any

func RegisterRoutes(r *mux.Router, db *sql.DB) {
	h := &Handler{db: db}

	// Core endpoints
	r.HandleFunc("/health", h.Health).Methods(http.MethodGet)
	r.HandleFunc("/tables", h.ListTables).Methods(http.MethodGet)
	r.HandleFunc("/query", h.PostQuery).Methods(http.MethodPost)

	// Sampling endpoints
	r.HandleFunc("/samples/create", h.PostCreateSample).Methods(http.MethodPost)
	r.HandleFunc("/samples/stratified", h.PostCreateStratifiedSample).Methods(http.MethodPost)

	// Sketch endpoints
	r.HandleFunc("/sketches/create", h.PostCreateSketch).Methods(http.MethodPost)
	r.HandleFunc("/sketches", h.GetSketches).Methods(http.MethodGet)

	// ML Learning endpoints
	r.HandleFunc("/ml/stats", h.GetLearningStats).Methods(http.MethodGet)
}

type Handler struct {
	db *sql.DB
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	_ "modernc.org/sqlite"

	"github.com/sahithikokkula/Hackathon-E6Data/aqe/pkg/api"
	"github.com/sahithikokkula/Hackathon-E6Data/aqe/pkg/storage"
)

func main() {
	// Database path (SQLite for MVP). Uses local file aqe.sqlite.
	dbPath := os.Getenv("AQE_DB_PATH")
	if dbPath == "" {
		dbPath = "aqe.sqlite"
	}

	log.Printf("Using database path: %s", dbPath)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("failed to open sqlite db: %v", err)
	}
	defer db.Close()

	// Pragmas for better performance
	db.Exec("PRAGMA journal_mode=WAL;")
	db.Exec("PRAGMA synchronous=NORMAL;")

	if err := storage.EnsureMetaTables(context.Background(), db); err != nil {
		log.Fatalf("failed to ensure meta tables: %v", err)
	}

	r := mux.NewRouter()
	api.RegisterRoutes(r, db)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	log.Printf("AQE server listening on http://localhost:%s", port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
	fmt.Println("server stopped")
}

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/ViMinhThang/LRdb/internal/engine"
)

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	// 1. Open/initialize the database engine
	db, err := engine.OpenDB("wal.log", 10)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// 2. Define the PUT handler
	http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		var req PutRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.Key == "" {
			http.Error(w, "key cannot be empty", http.StatusBadRequest)
			return
		}

		if err := db.Put(req.Key, []byte(req.Value)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK\n"))
	})

	// 3. Define the GET handler
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
			return
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Missing key parameter", http.StatusBadRequest)
			return
		}

		value, found := db.Get(key)
		if !found {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "text/plain")
		w.Write(value)
	})

	// 4. Start the server
	port := ":8080"
	fmt.Printf("LRdb serving HTTP API on port %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

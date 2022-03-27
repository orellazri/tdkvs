package master

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// Start master server
func Start(port int) {
	log.Printf("Master server starting on port %v...", port)

	router := mux.NewRouter()
	router.HandleFunc("/", indexHandler).Methods("GET")
	router.HandleFunc("/get/{key}", getKeyHandler).Methods("GET")
	http.Handle("/", router)
	http.ListenAndServe(fmt.Sprintf("localhost:%v", port), router)
}

// Handle index route
func indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "tdkvs master server running")
}

// Handle retrieveing keys
func getKeyHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fmt.Fprintf(w, "Key: %v\n", vars["key"])
}

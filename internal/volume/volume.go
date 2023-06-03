package volume

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// Config struct to unmarshal from yaml file for the volume server
type Config struct {
	Port int    // Server port
	Path string // Path to file storage directory
}

// Context for global state
type context struct {
	fs *fileStorage
}

// Start volume server
func Start(config *Config) {
	log.Printf("Volume server starting on port %v...", config.Port)

	fs := &fileStorage{
		path: config.Path,
	}
	context := &context{
		fs,
	}

	router := mux.NewRouter()
	router.HandleFunc("/", indexHandler).Methods("GET")
	router.HandleFunc("/get/{key}", func(w http.ResponseWriter, r *http.Request) {
		getKeyHandler(w, r, context)
	}).Methods("GET")
	router.HandleFunc("/set/{key}", func(w http.ResponseWriter, r *http.Request) {
		setKeyHandler(w, r, context)
	}).Methods("PUT")
	router.HandleFunc("/delete/{key}", func(w http.ResponseWriter, r *http.Request) {
		deleteKeyHandler(w, r, context)
	}).Methods("DELETE")
	http.Handle("/", router)
	http.ListenAndServe(fmt.Sprintf("localhost:%v", config.Port), router)
}

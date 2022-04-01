package volume

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/orellazri/tdkvs/utils"
)

type context struct {
	fs *fileStorage
}

// Start volume server
func Start(config *utils.VolumeConfig) {
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

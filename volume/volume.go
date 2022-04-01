package volume

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

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

// Handle index route
func indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "tdkvs volume server running")
}

// Handle retrieveing keys
func getKeyHandler(w http.ResponseWriter, r *http.Request, c *context) {
	key := mux.Vars(r)["key"]
	hash := r.URL.Query().Get("hash")
	if key == "" || hash == "" {
		http.Error(w, "Invalid key or hash", http.StatusBadRequest)
		return
	}

	as := r.URL.Query().Get("as")

	value, err := c.fs.get(key, hash)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, fmt.Sprintf("Key \"%v\" does not exist", key), http.StatusInternalServerError)
		} else {
			http.Error(w, fmt.Sprintf("An error occurred while retrieving key \"%v\"", key), http.StatusInternalServerError)
			log.Println(err)
		}

		return
	}

	log.Printf("Got key \"%v\"", key)

	switch as {
	case "int":
		fmt.Fprintf(w, "%v", binary.BigEndian.Uint64(value))
	case "string":
		fmt.Fprintf(w, "%v", string(value))
	default:
		fmt.Fprintf(w, "%v", value)
	}
}

// Handle settings keys
func setKeyHandler(w http.ResponseWriter, r *http.Request, c *context) {
	key := mux.Vars(r)["key"]
	hash := r.URL.Query().Get("hash")
	if key == "" || hash == "" {
		http.Error(w, "Invalid key or hash", http.StatusBadRequest)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "An error occurred while parsing request body", http.StatusInternalServerError)
		log.Println(err)
		return
	}

	err = c.fs.set(key, hash, data)
	if err != nil {
		http.Error(w, fmt.Sprintf("An error occurred while setting key \"%v\"", key), http.StatusInternalServerError)
		log.Println(err)
		return
	}

	log.Printf("Set key \"%v\"", key)

	fmt.Fprintf(w, "success")
}

// Handle deleting keys
func deleteKeyHandler(w http.ResponseWriter, r *http.Request, c *context) {
	key := mux.Vars(r)["key"]
	hash := r.URL.Query().Get("hash")
	if key == "" || hash == "" {
		http.Error(w, "Invalid key or hash", http.StatusBadRequest)
		return
	}

	err := c.fs.delete(key, hash)
	if err != nil {
		http.Error(w, fmt.Sprintf("An error occurred while deleting key \"%v\"", key), http.StatusInternalServerError)
		log.Println(err)
		return
	}

	log.Printf("Deleted key \"%v\"", key)

	fmt.Fprintf(w, "ok")
}

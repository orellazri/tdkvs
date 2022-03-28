package master

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/gorilla/mux"
	"github.com/orellazri/tdkvs/utils"
)

type context struct {
	config *utils.Config
	db     *badger.DB
}

// Start master server
func Start(port int, config *utils.Config) {
	log.Printf("Master server starting on port %v...", port)

	// Initialize BadgerDB
	options := badger.DefaultOptions("badger")
	options.Logger = nil
	db, err := badger.Open(options)
	utils.AbortOnError(err)
	defer db.Close()

	// The context holds the global state for the master server
	context := &context{
		config,
		db,
	}

	// TEMP: Show all keys
	// err = db.View(func(txn *badger.Txn) error {
	// 	opts := badger.DefaultIteratorOptions
	// 	opts.PrefetchSize = 10
	// 	it := txn.NewIterator(opts)
	// 	defer it.Close()
	// 	for it.Rewind(); it.Valid(); it.Next() {
	// 		item := it.Item()
	// 		k := item.Key()
	// 		err := item.Value(func(v []byte) error {
	// 			fmt.Printf("key=%s, value=%s\n", k, v)
	// 			return nil
	// 		})
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// 	return nil
	// })

	router := mux.NewRouter()
	router.HandleFunc("/", indexHandler).Methods("GET")
	router.HandleFunc("/get/{key}", func(w http.ResponseWriter, r *http.Request) {
		getKeyHandler(w, r, context)
	}).Methods("GET")
	router.HandleFunc("/set/{key}", func(w http.ResponseWriter, r *http.Request) {
		setKeyHandler(w, r, context)
	}).Methods("PUT")
	http.Handle("/", router)
	http.ListenAndServe(fmt.Sprintf("localhost:%v", port), router)
}

// Handle index route
func indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "tdkvs master server running")
}

// Handle retrieveing keys
func getKeyHandler(w http.ResponseWriter, r *http.Request, c *context) {
	key := mux.Vars(r)["key"]
	as := r.URL.Query().Get("as")

	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		// Key exists
		err = item.Value(func(v []byte) error {
			// TODO: Retrieve from volume server
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			// Key doesn't exist. Retrieve from volume server

			// Choose a volume server using jump consistent hash
			hash, numVolume := utils.ChooseBucketString(key, int32(len(c.config.Volumes)))

			// Send request to volume server
			resp, err := http.Get(fmt.Sprintf("%v/get/%v?hash=%v&as=%v", c.config.Volumes[numVolume], key, hash, as))
			if err != nil {
				http.Error(w, fmt.Sprintf("An error occurred while retrieving key \"%v\"", key), http.StatusInternalServerError)
				return
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				http.Error(w, fmt.Sprintf("An error occurred while retrieving key \"%v\"", key), http.StatusInternalServerError)
				return
			}

			fmt.Fprintf(w, "%v", string(body))
		} else {
			http.Error(w, fmt.Sprintf("An error occurred while retrieving key \"%v\"", key), http.StatusInternalServerError)
		}
	}

}

// Handle setting keys
func setKeyHandler(w http.ResponseWriter, r *http.Request, c *context) {
	key := mux.Vars(r)["key"]

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "An error occurred while parsing request body", http.StatusInternalServerError)
		return
	}
	value := string(data)

	hash, numVolume := utils.ChooseBucketString(value, int32(len(c.config.Volumes)))

	// Send request to volume server
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%v/set/%v?hash=%v", c.config.Volumes[numVolume], key, hash), strings.NewReader(value))
	if err != nil {
		http.Error(w, fmt.Sprintf("An error occurred while setting key \"%v\"", key), http.StatusInternalServerError)
		return
	}
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("An error occurred while setting key \"%v\"", key), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	fmt.Fprintf(w, "%v", resp.StatusCode)
}

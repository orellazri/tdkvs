package master

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"

	"github.com/dgraph-io/badger/v3"
	"github.com/gorilla/mux"
	"github.com/orellazri/tdkvs/utils"
)

type Context struct {
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
	context := Context{
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
		getKeyHandler(w, r, &context)
	}).Methods("GET")
	http.Handle("/", router)
	http.ListenAndServe(fmt.Sprintf("localhost:%v", port), router)
}

// Handle index route
func indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "tdkvs master server running")
}

// Handle retrieveing keys
func getKeyHandler(w http.ResponseWriter, r *http.Request, c *Context) {
	key := mux.Vars(r)["key"]

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
		if err == badger.ErrKeyNotFound {
			// Key doesn't exist

			// Convert string key to uint64
			// TODO: Check if this is safe for concurrent use
			hahser := fnv.New64()
			hahser.Write([]byte(key))
			keyAsInt := hahser.Sum64()

			// Choose a volume server using jump consistent hash
			numVolume := utils.JumpConsisntentHash(keyAsInt, int32(len(c.config.Volumes)))

			// Request from volume server
			resp, err := http.Get(fmt.Sprintf("%v/get/%v", c.config.Volumes[numVolume], key))
			if err != nil {
				fmt.Fprintf(w, "An error occurred while retrieving key \"%v\"", key)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				fmt.Fprintf(w, "An error occurred while retrieving key \"%v\"", key)
			}

			fmt.Fprintf(w, "%v", string(body))
		} else {
			fmt.Fprintf(w, "An error occurred while retrieving key \"%v\"", key)
		}
	}

}

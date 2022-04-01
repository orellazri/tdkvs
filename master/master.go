package master

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/dgraph-io/badger/v3"
	"github.com/gorilla/mux"
	"github.com/orellazri/tdkvs/utils"
)

type context struct {
	config *utils.MasterConfig
	db     *badger.DB
}

// Start master server
func Start(config *utils.MasterConfig) {
	log.Printf("Master server starting on port %v...", config.Port)

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

	// Check number of volume servers and rebalance if needed
	var metaNumVolumes int
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("_meta_num_volumes"))
		if err != nil {
			return err
		}

		item.Value(func(v []byte) error {
			metaNumVolumes = int(binary.BigEndian.Uint32(v))
			return nil
		})

		return nil
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			// No num volumes meta key, set it
			err := db.Update(func(txn *badger.Txn) error {
				var numVolumeBytes [4]byte
				binary.BigEndian.PutUint32(numVolumeBytes[0:4], uint32(len(config.Volumes)))
				err := txn.Set([]byte("_meta_num_volumes"), numVolumeBytes[:])
				return err
			})
			utils.AbortOnError(err)
		} else {
			utils.AbortOnError(err)
		}
	} else {
		// Num volumes meta key found. Compare with current amount of volume servers
		// and relanace if needed
		if metaNumVolumes != len(config.Volumes) {
			if len(config.Volumes) < metaNumVolumes {
				log.Fatal("Current amount of volume servers is less than the last amount! Aborting")
				os.Exit(1)
			}

			log.Println("Rebalancing...")
			err := rebalance(context)
			utils.AbortOnError(err)

			// Set metakey to new number of volume servers
			err = db.Update(func(txn *badger.Txn) error {
				var numVolumeBytes [4]byte
				binary.BigEndian.PutUint32(numVolumeBytes[0:4], uint32(len(config.Volumes)))
				err := txn.Set([]byte("_meta_num_volumes"), numVolumeBytes[:])
				return err
			})
			utils.AbortOnError(err)

			log.Println("Rebalancing done!")
		}
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

package master

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/gorilla/mux"
	"github.com/orellazri/tdkvs/utils"
)

// Config struct to unmarshal from yaml file for the master server
type Config struct {
	Port         int      // Server port
	Volumes      []string // List of volume servers
	DeleteVolume int      // Optional. Volume server to delete if we are in volume delete mode
}

// Context for global state
type context struct {
	config *Config
	db     *badger.DB
}

// Opearting mode enum
const (
	Normal = iota
	DeleteVolume
)

// Start master server
func Start(config *Config, mode int) {
	if mode == DeleteVolume {
		deleteVolume(0)
		return
	}

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

// Rebalance keys in volume servers
func rebalance(c *context) error {
	err := c.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())

			// Skip meta keys
			if strings.HasPrefix(key, "_meta") {
				continue
			}

			// Check if key needs to be moved
			err := item.Value(func(v []byte) error {
				currentVolume := binary.BigEndian.Uint32(v)
				hash, newVolume := utils.ChooseBucketString(key, int32(len(c.config.Volumes)))
				if newVolume != currentVolume {
					log.Printf("Moving \"%v\" from volume server %v to %v\n", key, currentVolume, newVolume)

					// Get value from current volume server
					resp, err := http.Get(fmt.Sprintf("%v/get/%v?hash=%v", c.config.Volumes[currentVolume], key, hash))
					if err != nil {
						return err
					}
					if resp.StatusCode != 200 {
						return errors.New("respose from volume server is not 200 OK")
					}

					body, err := io.ReadAll(resp.Body)
					if err != nil {
						return err
					}

					value := body

					resp.Body.Close()

					// Delete key from current volume server
					req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%v/delete/%v?hash=%v", c.config.Volumes[currentVolume], key, hash), strings.NewReader(""))
					if err != nil {
						return err
					}
					client := http.Client{}
					resp, err = client.Do(req)
					if err != nil {
						return err
					}

					if resp.StatusCode != 200 {
						return errors.New("response from volue server is not 200 OK")
					}
					resp.Body.Close()

					// Set key in new volume server
					req, err = http.NewRequest(http.MethodPut, fmt.Sprintf("%v/set/%v?hash=%v", c.config.Volumes[newVolume], key, hash), bytes.NewBuffer(value))
					if err != nil {
						return err
					}
					client = http.Client{}
					resp, err = client.Do(req)
					if err != nil {
						return err
					}

					if resp.StatusCode != 200 {
						return errors.New("response from volue server is not 200 OK")
					}
					resp.Body.Close()

					// Update metakey in db
					err = c.db.Update(func(txn *badger.Txn) error {
						var numVolumeBytes [4]byte
						binary.BigEndian.PutUint32(numVolumeBytes[0:4], uint32(newVolume))
						err := txn.Set([]byte(key), numVolumeBytes[:])
						return err
					})
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

// Move keys from a volume server
func deleteVolume(numVolume int) {

}

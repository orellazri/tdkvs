package master

import (
	"bytes"
	"encoding/binary"
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

// Handle index route
func indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "tdkvs master server running")
}

// Handle retrieveing keys
func getKeyHandler(w http.ResponseWriter, r *http.Request, c *context) {
	key := mux.Vars(r)["key"]
	as := r.URL.Query().Get("as")

	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		// Key exists. Retrieve from volume server
		err = item.Value(func(v []byte) error {
			numVolume := binary.BigEndian.Uint32(v)
			hash := utils.HashString(key)

			// Send request to volume server
			resp, err := http.Get(fmt.Sprintf("%v/get/%v?hash=%v&as=%v", c.config.Volumes[numVolume], key, hash, as))
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				return errors.New("respose from volume server is not 200 OK")
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}

			log.Printf("Got key \"%v\" from volume server %v", key, numVolume)
			w.Write(body)
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			// Key doesn't exist
			http.Error(w, fmt.Sprintf("Key \"%v\" does not exist", key), http.StatusBadRequest)
		} else {
			http.Error(w, fmt.Sprintf("An error occurred while retrieving key \"%v\"", key), http.StatusInternalServerError)
			log.Println(err)
		}
	}
}

// Handle setting keys
func setKeyHandler(w http.ResponseWriter, r *http.Request, c *context) {
	key := mux.Vars(r)["key"]

	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	// Read request body
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "An error occurred while parsing request body", http.StatusInternalServerError)
		log.Println(err)
		return
	}
	value := string(data)
	if value == "" {
		http.Error(w, "Request body is required", http.StatusBadRequest)
		return
	}

	// Choose bucket and generate ahsh
	hash, numVolume := utils.ChooseBucketString(key, int32(len(c.config.Volumes)))

	// Send request to volume server
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%v/set/%v?hash=%v", c.config.Volumes[numVolume], key, hash), strings.NewReader(value))
	if err != nil {
		http.Error(w, fmt.Sprintf("An error occurred while setting key \"%v\"", key), http.StatusInternalServerError)
		log.Println(err)
		return
	}
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("An error occurred while setting key \"%v\"", key), http.StatusInternalServerError)
		log.Println(err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		http.Error(w, fmt.Sprintf("An error occurred while setting key \"%v\"", key), http.StatusInternalServerError)
		return
	}

	// Key is set, add metakey to db
	err = c.db.Update(func(txn *badger.Txn) error {
		var numVolumeBytes [4]byte
		binary.BigEndian.PutUint32(numVolumeBytes[0:4], uint32(numVolume))
		err := txn.Set([]byte(key), numVolumeBytes[:])
		return err
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("An error occurred while setting key \"%v\"", key), http.StatusInternalServerError)
		log.Println(err)
		return
	}

	log.Printf("Set key \"%v\" in volume server %v", key, numVolume)
	fmt.Fprintf(w, "ok")
}

// Handle deleting keys
func deleteKeyHandler(w http.ResponseWriter, r *http.Request, c *context) {
	key := mux.Vars(r)["key"]
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	var numVolume uint32

	// Check if key exists in db
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		item.Value(func(v []byte) error {
			// Key exists. Get volume number from db
			numVolume = binary.BigEndian.Uint32(v)
			return nil
		})

		return nil
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			// Key doesn't exist
			http.Error(w, fmt.Sprintf("Key \"%v\" does not exist", key), http.StatusBadRequest)
			return
		} else {
			http.Error(w, fmt.Sprintf("An error occurred while deleting key \"%v\"", key), http.StatusInternalServerError)
			log.Println(err)
			return
		}
	}

	// Key exists
	hash := utils.HashString(key)

	// Send request to volume server
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%v/delete/%v?hash=%v", c.config.Volumes[numVolume], key, hash), strings.NewReader(""))
	if err != nil {
		http.Error(w, fmt.Sprintf("An error occurred while deleting key \"%v\"", key), http.StatusInternalServerError)
		log.Println(err)
		return
	}
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("An error occurred while deleting key \"%v\"", key), http.StatusInternalServerError)
		log.Println(err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		http.Error(w, fmt.Sprintf("An error occurred while deleting key \"%v\"", key), http.StatusInternalServerError)
		return
	}

	// Key is deleted. Delete it from db as well
	err = c.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(key))
		return err
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("An error occurred while deleting key \"%v\"", key), http.StatusInternalServerError)
		log.Println(err)
		return
	}

	log.Printf("Deleted key \"%v\" from volume server %v", key, numVolume)
	fmt.Fprintf(w, "ok")
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

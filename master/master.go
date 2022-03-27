package master

import (
	"fmt"
	"log"
	"net/http"

	"github.com/dgraph-io/badger/v3"
	"github.com/gorilla/mux"
)

type Context struct {
	db *badger.DB
}

// Start master server
func Start(port int) {
	log.Printf("Master server starting on port %v...", port)

	db, err := badger.Open(badger.DefaultOptions("badger"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	context := Context{
		db,
	}

	err = db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte("answer"), []byte("42"))
		return err
	})

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

		err = item.Value(func(v []byte) error {
			fmt.Fprintf(w, "Key: %v\nValue: %v", key, string(v))
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		fmt.Fprintf(w, "An error occurred while retrieving key \"%v\"", key)
	}
}

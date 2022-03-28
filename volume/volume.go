package volume

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

type context struct {
	fs *fileStorage
}

// Start master server
func Start(port int) {
	log.Printf("Volume server starting on port %v...", port)

	fs := &fileStorage{
		path: "volume1/",
	}
	context := &context{
		fs,
	}

	router := mux.NewRouter()
	router.HandleFunc("/", indexHandler).Methods("GET")
	router.HandleFunc("/get/{key}", func(w http.ResponseWriter, r *http.Request) {
		getKeyHandler(w, r, context)
	}).Methods("GET")
	http.Handle("/", router)
	http.ListenAndServe(fmt.Sprintf("localhost:%v", port), router)
}

// Handle index route
func indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "tdkvs volume server running")
}

// Handle retrieveing keys
func getKeyHandler(w http.ResponseWriter, r *http.Request, c *context) {
	key := mux.Vars(r)["key"]

	c.fs.test()

	fmt.Fprintf(w, "get %v", key)
}

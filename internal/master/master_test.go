package master

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/gorilla/mux"
	"github.com/orellazri/tdkvs/internal/utils"
)

func TestGetNonexistentKey(t *testing.T) {
	// Initialize BadgerDB
	options := badger.DefaultOptions(os.TempDir())
	options.Logger = nil
	db, err := badger.Open(options)
	utils.AbortOnError(err)
	defer db.Close()

	context := &context{
		config: &Config{Port: 3000, Volumes: []string{"http://localhost:3001"}},
		db:     db,
	}

	router := mux.NewRouter()
	router.HandleFunc("/get/{key}", func(w http.ResponseWriter, r *http.Request) {
		getKeyHandler(w, r, context)
	}).Methods("GET")

	server := httptest.NewServer(router)
	defer server.Close()

	client := http.DefaultClient
	resp, err := client.Get(server.URL + "/get/test_nonexistent_key")
	if err != nil {
		t.Error(err)
	}
	if resp.StatusCode != 400 {
		t.Error("response status code is not 400 Bad Request")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Error(err)
	}
	value := string(body)
	if !strings.Contains(value, "does not exist") {
		t.Error("response does not contain: \"does not exist\"")
	}
}

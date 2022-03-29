package volume

import (
	"bytes"
	"os"
	"path"
	"testing"
)

func TestKeyToPath(t *testing.T) {
	fs := fileStorage{path: path.Join("tmp", "volume1")}
	actual := fs.keyToPath("test", "123456789")

	expected := path.Join("tmp", "volume1", "12", "1234", "123456789_test")
	if actual != expected {
		t.Errorf("Expected %v but got %v", expected, actual)
	}
}

func TestSetGetKey(t *testing.T) {
	tempDir := os.TempDir()
	fs := fileStorage{path: tempDir}
	key := "test"
	hash := "1234556789"
	value := []byte("testvalue")

	fs.set(key, hash, value)

	actual, err := fs.get(key, hash)
	if err != nil {
		t.Error(err)
	}

	if bytes.Compare(actual, value) != 0 {
		t.Errorf("Expected %v but got %v", value, actual)
	}
}

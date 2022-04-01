package volume

import (
	"bytes"
	"errors"
	"os"
	"path"
	"testing"
)

func TestKeyToPath(t *testing.T) {
	fs := fileStorage{path: path.Join("tmp", "volume1")}
	actual := fs.keyToPath("test", "123456789")

	expected := path.Join("tmp", "volume1", "12", "1234", "123456789_test")
	if actual != expected {
		t.Errorf("expected %v but got %v", expected, actual)
	}
}

func TestGetNonexistentKey(t *testing.T) {
	tempDir := os.TempDir()
	fs := fileStorage{path: tempDir}
	key := "test"
	hash := "123456789"

	_, err := fs.get(key, hash)
	if err == nil {
		t.Error("expected to fail on getting nonexistent key")
	}
}

func TestSetGetKey(t *testing.T) {
	tempDir := os.TempDir()
	fs := fileStorage{path: tempDir}
	key := "test"
	hash := "123456789"
	value := []byte("testvalue")

	fs.set(key, hash, value)

	actual, err := fs.get(key, hash)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(actual, value) {
		t.Errorf("expected %v but got %v", value, actual)
	}
}

func TestSetDeleteKey(t *testing.T) {
	tempDir := os.TempDir()
	fs := fileStorage{path: tempDir}
	key := "test"
	hash := "123456789"
	value := []byte("testvalue")

	fs.set(key, hash, value)

	err := fs.delete(key, hash)
	if err != nil {
		t.Error(err)
	}

	_, err = os.Stat(path.Join(tempDir, "12", "1234", "123456789_test"))
	if err == nil {
		t.Error("key still exists after deletion")
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Error(err)
	}
}

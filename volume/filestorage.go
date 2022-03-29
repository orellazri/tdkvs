package volume

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
)

type fileStorage struct {
	path string
}

// Return a path, given a key and the key's hash
// The path is the root volume path, the first two characters of the hash,
// the first four characters of the hash, and the hash followed by
// the first 10 characters of the key
// i.e. volume/17/1727/17270204244788214835_answer
func (fs *fileStorage) keyToPath(key string, hash string) string {
	truncatedKey := key
	if len(key) > 10 {
		truncatedKey = key[:10]
	}
	return filepath.Join(fs.path, hash[:2], hash[:4], fmt.Sprintf("%v_%v", hash, truncatedKey))
}

// Retrieve a key
func (fs *fileStorage) get(key string, hash string) ([]byte, error) {
	path := fs.keyToPath(key, hash)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Set value to key
func (fs *fileStorage) set(key string, hash string, value []byte) error {
	// TODO: Check mutex
	filePath := fs.keyToPath(key, hash)

	// Make directores and write to file
	err := os.MkdirAll(path.Dir(filePath), 0777)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(value)
	if err != nil {
		return err
	}

	return nil
}

// Delete key
func (fs *fileStorage) delete(key string, hash string) error {
	path := fs.keyToPath(key, hash)
	err := os.Remove(path)
	return err
}

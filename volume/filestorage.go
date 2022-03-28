package volume

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type fileStorage struct {
	path string
}

// Return a path, given a key and the key's hash
// The path is the root volume path, the first two characters of the hash,
// the first four characters of the hash, and the hash followed by the key
// i.e. volume/17/1727/17270204244788214835_answer
func (fs *fileStorage) keyToPath(key string, hash string) (string, error) {
	if len(hash) != 64 {
		return "", errors.New("Invalid hash length")
	}
	return filepath.Join(fs.path, hash[:2], hash[:4], fmt.Sprintf("%v_%v", hash, key)), nil
}

// Retrieve a key
func (fs *fileStorage) get(key string, hash string) ([]byte, error) {
	path, err := fs.keyToPath(key, hash)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

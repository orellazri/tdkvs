package volume

import "fmt"

type fileStorage struct {
	path string
}

func (f *fileStorage) test() {
	fmt.Printf("Hi from %v\n", f.path)
}

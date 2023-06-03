package utils

import (
	"hash/fnv"
	"log"
)

// Exit with a fatal log if an error occurred
func AbortOnError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Use jump consistent has to choose a bucket
// for a given key
// Outputs a number in the range [0, numOfBuckets]
func ChooseBucket(key uint64, numOfBuckets int32) int32 {
	var b, j int64

	for j < int64(numOfBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}

// Call ChooseBucket (jump consistent hash) after converting a string key
// to uint64
// Returns the hash (key as uint64) and the bucket number
func ChooseBucketString(key string, numOfBuckets int32) (uint64, uint32) {
	hash := HashString(key)
	return hash, uint32(ChooseBucket(hash, numOfBuckets))
}

func HashString(key string) uint64 {
	// TODO: Check if this is safe for concurrent use
	hahser := fnv.New64()
	hahser.Write([]byte(key))
	hash := hahser.Sum64()
	return hash
}

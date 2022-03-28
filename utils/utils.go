package utils

import "log"

// Config struct to unmarshal from the yaml file
type Config struct {
	Volumes []string
}

// Exit with a fatal log if an error occurred
func AbortOnError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Jump Consistent Hash
// Takes a 64bit key and number of buckets
// Outputs a number in the range [0, numOfBuckets]
func JumpConsisntentHash(key uint64, numOfBuckets int32) int32 {
	var b, j int64

	for j < int64(numOfBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}

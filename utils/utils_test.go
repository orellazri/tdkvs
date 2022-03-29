package utils

import (
	"math/rand"
	"testing"
)

func TestChooseBucketInRange(t *testing.T) {
	for i := 0; i < 1000; i++ {
		key := rand.Uint64()
		numBucket := ChooseBucket(key, 10)
		if numBucket < 0 || numBucket >= 10 {
			t.Error("Number of bucket is not in range [0, 10)")
		}
	}
}

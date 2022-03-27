package utils

// Jump Consistent Hash
// Takes a 64bit key and number of buckets
// Outputs a number in the range [0, num_of_buckets]
func Hash(key uint64, num_of_buckets int32) int32 {
	var b, j int64

	for j < int64(num_of_buckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}

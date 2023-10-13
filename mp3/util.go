package main

import (
	"hash"
	"hash/fnv"
)

var fnvHash hash.Hash32 = fnv.New32a()

// Hash string s to an integer between 0 and N-1
func GetHash(s string, N int) int {
	fnvHash.Write([]byte(s))
	hashValue := fnvHash.Sum32()
	return int(hashValue % uint32(N))
}


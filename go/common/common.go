package common

import (
	"encoding/base64"
	log "github.com/sirupsen/logrus"
	"hash"
	"hash/fnv"
	"math/rand"
)

type Notifier interface {
	HandleNodeJoin(node *Node, memberID string)
	HandleNodeLeave(node *Node, memberID string)
}

func RandomChoice[T comparable](arr []T) T {
	return arr[rand.Intn(len(arr))]
}

func Abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func Min[T int](a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max[T int](a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func Shuffle[T comparable](slice []T) []T {
	rand.Shuffle(len(slice), func(i, j int) {
		slice[i], slice[j] = slice[j], slice[i]
	})
	return slice
}

func GetKeys[T comparable, U any](m map[T]U) []T {
	keys := []T{}
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func GetValues[T comparable, U any](m map[T]U) []U {
	values := []U{}
	for _, v := range m {
		values = append(values, v)
	}
	return values
}

func MakeSet[T comparable](values []T) map[T]bool {
	res := make(map[T]bool)
	for _, value := range values {
		res[value] = true
	}
	return res
}

// Hash string s to an integer in [0,N)
func GetHash(s string, N int) int {
	var fnvHash hash.Hash32 = fnv.New32a()
	fnvHash.Write([]byte(s))
	hashValue := fnvHash.Sum32()
	return int(hashValue % uint32(N))
}

func EncodeFilename(name string) string {
	return base64.StdEncoding.EncodeToString([]byte(name))
}

func DecodeFilename(name string) string {
	decodedBytes, err := base64.StdEncoding.DecodeString(name)
	if err != nil {
		log.Warn("Error decoding string:", err)
		return ""
	}
	return string(decodedBytes)
}

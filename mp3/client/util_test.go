package main

import (
	"fmt"
	"testing"
)

func TestUtil(t *testing.T) {
	size := int64(1024 * 1024) // 1 MB
	fmt.Printf("size: %d, num blocks: %d\n", size, GetNumFileBlocks(size))

	size = 6400
	fmt.Printf("size: %d, num blocks: %d\n", size, GetNumFileBlocks(size))

	SplitFileIntoBlocks("data", "blocks")
}

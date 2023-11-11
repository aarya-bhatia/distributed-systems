package maplejuice

import (
	"cs425/common"
	log "github.com/sirupsen/logrus"
	"testing"
)

func TestSplitter(t *testing.T) {
	dirname := "../data/dataset"
	res, err := common.GetFilesInDirectory(dirname)
	if err != nil {
		t.Fail()
	}

	ms := NewMultiFileSplitter(64)

	log.Println("Hello")

	for _, entry := range res {
		log.Println("Adding", entry.Name, entry.Size)
		if !ms.AddFile(dirname + "/" + entry.Name) {
			t.Fail()
		}
	}

	count := 0
	for {
		lines, ok := ms.Next()
		if !ok {
			break
		}

		count += len(lines)
		log.Println(len(lines), " lines")
	}

	log.Println("Total", count, "lines")
}

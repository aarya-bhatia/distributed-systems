package client

import (
	"cs425/common"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os/exec"
	"testing"
)

func getClient() *SDFSClient {
	common.Cluster = common.SDFSLocalCluster
	node := common.Cluster[0]
	addr := common.GetAddress(node.Hostname, node.RPCPort)
	return NewSDFSClient(addr)
}

func TestClient(t *testing.T) {
	sdfsClient := getClient()

	inputFiles := []string{"data/small", "data/mid", "data/large"}
	remoteFiles := []string{"small", "mid", "large"}
	outputFiles := []string{"small.out", "mid.out", "large.out"}

	for i := range inputFiles {
		reader, err := NewFileReader(inputFiles[i])
		if err != nil {
			log.Println(err)
			t.Fail()
		}

		if err := sdfsClient.WriteFile(reader, remoteFiles[i], common.FILE_TRUNCATE); err != nil {
			log.Println(err)
			t.Fail()
		}

		writer, err := NewFileWriter(outputFiles[i])
		if err != nil {
			log.Println(err)
			t.Fail()
		}

		if err := sdfsClient.DownloadFile(writer, remoteFiles[i]); err != nil {
			log.Println(err)
			t.Fail()
		}

		if err := exec.Command("diff", inputFiles[i], outputFiles[i]).Run(); err != nil {
			log.Println(err)
			t.Fail()
		}
	}
}

func TestAppend(t *testing.T) {
	sdfsClient := getClient()

	assert.Nil(t, sdfsClient.DeleteFile("test"))

	for i := 0; i < 1; i++ {
		assert.Nil(t, sdfsClient.WriteFile(NewByteReader([]byte("Hello\n")), "test", common.FILE_APPEND))
		assert.Nil(t, sdfsClient.WriteFile(NewByteReader([]byte("World\n")), "test", common.FILE_APPEND))
	}

	metadata, err := sdfsClient.GetFile("test")
	assert.Nil(t, err)
	assert.Equal(t, metadata.File.FileSize, 12)

	w := NewByteWriter()
	assert.Nil(t, sdfsClient.DownloadFile(w, "test"))
	assert.Equal(t, w.String(), "Hello\nWorld\n")
}

func TestLargeFile(t *testing.T) {
	sdfsClient := getClient()

	assert.Nil(t, sdfsClient.DeleteFile("large"))

	reader, err := NewFileReader("data/large")
	assert.Nil(t, err)

	err = sdfsClient.WriteFile(reader, "large", common.FILE_TRUNCATE)
	assert.Nil(t, err)

	err = sdfsClient.WriteFile(reader, "large", common.FILE_APPEND)
	assert.Nil(t, err)

	file, err := sdfsClient.GetFile("large")
	assert.Nil(t, err)
	assert.True(t, file.File.FileSize == 2*reader.Size())
	assert.True(t, len(file.Blocks) == common.GetNumFileBlocks(int64(2*reader.Size())))
	assert.True(t, file.Blocks[0].Size == common.BLOCK_SIZE)
	assert.True(t, len(file.Blocks[0].Replicas) > 0)
}

package client

import (
	"cs425/common"
	"cs425/filesystem/server"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os/exec"
	"testing"
)

func TestClient(t *testing.T) {
	common.Cluster = common.SDFSLocalCluster
	node := common.Cluster[0]
	addr := common.GetAddress(node.Hostname, node.RPCPort)
	sdfsClient := NewSDFSClient(addr)

	inputFiles := []string{"data/small", "data/mid", "data/large"}
	remoteFiles := []string{"small", "mid", "large"}
	outputFiles := []string{"small.out", "mid.out", "large.out"}

	for i := range inputFiles {
		reader, err := NewFileReader(inputFiles[i])
		if err != nil {
			t.Fail()
		}

		if err := sdfsClient.WriteFile(reader, remoteFiles[i], server.FILE_TRUNCATE); err != nil {
			log.Println(err)
			t.Fail()
		}

		if err := sdfsClient.DownloadFile(outputFiles[i], remoteFiles[i]); err != nil {
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
	common.Cluster = common.SDFSLocalCluster
	node := common.Cluster[0]
	addr := common.GetAddress(node.Hostname, node.RPCPort)
	sdfsClient := NewSDFSClient(addr)

	for i := 0; i < 1; i++ {
		assert.Nil(t, sdfsClient.WriteFile(NewByteReader([]byte("Hello\n")), "test", server.FILE_APPEND))
		assert.Nil(t, sdfsClient.WriteFile(NewByteReader([]byte("World\n")), "test", server.FILE_APPEND))
	}

	metadata, err := sdfsClient.GetFile("test")
	assert.Nil(t, err)
	assert.Equal(t, metadata.File.FileSize, 12)
}

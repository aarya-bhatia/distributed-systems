package client

import (
	"cs425/common"
	log "github.com/sirupsen/logrus"
	"os/exec"
	"testing"
)

func TestClient(t *testing.T) {
	common.Cluster = common.SDFSLocalCluster
	node := common.Cluster[0]
	server := common.GetAddress(node.Hostname, node.RPCPort)
	sdfsClient := NewSDFSClient(server)

	inputFiles := []string{"data/small", "data/mid", "data/large"}
	remoteFiles := []string{"small", "mid", "large"}
	outputFiles := []string{"small.out", "mid.out", "large.out"}

	for i := range inputFiles {
		if err := sdfsClient.UploadFile(inputFiles[i], remoteFiles[i]); err != nil {
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

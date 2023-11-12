package test

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"net/rpc"
	"os/exec"
	// "path/filepath"
	"testing"
)

func TestRPC(t *testing.T) {
	common.Cluster = common.SDFSLocalCluster
	node := common.Cluster[0]
	server := common.GetAddress(node.Hostname, node.RPCPort)
	fmt.Println("Server:", server)
	conn, err := rpc.Dial("tcp", server)
	if err != nil {
		log.Println(err)
		t.Fail()
	}
	defer conn.Close()
	reply := ""
	args := true
	err = conn.Call("Server.Ping", &args, &reply)
	if err != nil {
		log.Println(err)
		t.Fail()
	}
	assert.Equal(t, reply, "Pong")

	id := 0
	conn.Call("Server.GetLeader", &args, &id)
	assert.Equal(t, id, 1)
}

func TestClient(t *testing.T) {
	common.Cluster = common.SDFSLocalCluster
	node := common.Cluster[0]
	server := common.GetAddress(node.Hostname, node.RPCPort)
	sdfsClient := client.NewSDFSClient(server)

	inputFiles := []string{"data/small", "data/mid", "data/large"}
	outputFiles := []string{"small", "mid","large"}

	for i := range inputFiles {
		if err := sdfsClient.UploadFile(inputFiles[i], outputFiles[i]); err != nil {
			log.Println(err)
			t.Fail()
		}

		if err := sdfsClient.DownloadFile(outputFiles[i], outputFiles[i]); err != nil {
			log.Println(err)
			t.Fail()
		}

		if err := exec.Command("diff", inputFiles[i], outputFiles[i]).Run(); err != nil {
			log.Println(err)
			t.Fail()
		}
	}

}

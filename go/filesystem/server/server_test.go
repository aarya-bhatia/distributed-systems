package server

import (
	"cs425/common"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"net/rpc"
	"testing"
)

func TestRPC(t *testing.T) {
	common.Cluster = common.SDFSLocalCluster
	node := common.Cluster[0]
	addr := common.GetAddress(node.Hostname, node.RPCPort)
	fmt.Println("Server:", addr)
	conn, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Println(err)
		t.Fail()
	}
	defer conn.Close()
	reply := ""
	args := true
	err = conn.Call(RPC_PING, &args, &reply)
	if err != nil {
		log.Println(err)
		t.Fail()
	}
	assert.Equal(t, reply, "Pong")

	id := 0
	conn.Call(RPC_GET_LEADER, &args, &id)
	assert.Equal(t, id, 1)
}

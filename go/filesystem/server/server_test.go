package server

import (
	"cs425/common"
	"cs425/filesystem"
	"fmt"
	"net/rpc"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestMetadata(t *testing.T) {
	m := NewServerMetadata()

	b1 := "foo:1:0"
	b2 := "bar:1:0"

	m.UpdateBlockMetadata(BlockMetadata{
		Block:    b1,
		Replicas: []int{1, 2},
	})
	m.UpdateBlockMetadata(BlockMetadata{
		Block:    b1,
		Replicas: []int{3, 2},
	})
	m.UpdateBlockMetadata(BlockMetadata{
		Block:    b2,
		Replicas: []int{1},
	})

	assert.True(t, len(m.NodesToBlocks) == 3)
	assert.True(t, len(m.BlockToNodes) == 2)

	assert.True(t, m.BlockToNodes[b1].Contains(1))
	assert.True(t, m.BlockToNodes[b1].Contains(2))
	assert.True(t, m.BlockToNodes[b1].Contains(3))
	assert.False(t, m.BlockToNodes[b1].Contains(4))

	assert.True(t, m.BlockToNodes[b2].Contains(1))
	assert.False(t, m.BlockToNodes[b2].Contains(2))
	assert.False(t, m.BlockToNodes[b2].Contains(3))
	assert.False(t, m.BlockToNodes[b2].Contains(4))

	assert.True(t, m.NodesToBlocks[1].Contains(b1))
	assert.True(t, m.NodesToBlocks[1].Contains(b2))

	assert.True(t, m.NodesToBlocks[2].Contains(b1))
	assert.False(t, m.NodesToBlocks[2].Contains(b2))

	log.Println(m.GetMetadata(File{Filename: "foo", FileSize: 10, NumBlocks: 1}))
	log.Println(m.GetMetadata(File{Filename: "bar", FileSize: 10, NumBlocks: 1}))
}

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

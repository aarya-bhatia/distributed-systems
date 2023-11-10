package main

import (
	"cs425/common"
	"fmt"
	"net"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

var LEADER_NODE = common.LocalMapReduceCluster[0]

func printUsage() {
	fmt.Println("1) maple <maple_exe> <num_maples> <sdfs_prefix> <sdfs_src_dir>")
	fmt.Println("2) juice <juice_exe> <num_juices> <sdfs_prefix> <sdfs_dest_file> delete_input={0,1}")
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		return
	}

	if os.Args[1] == "maple" {
		if len(os.Args) < 6 {
			printUsage()
			return
		}

		request := strings.Join(os.Args[1:], " ")
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", LEADER_NODE.Hostname, LEADER_NODE.TCPPort))
		if err != nil {
			log.Fatal("Leader is offline")
		}

		if !common.SendMessage(conn, request) {
			log.Fatal("Failed to send request")
			return
		}

		buffer := make([]byte, common.MIN_BUFFER_SIZE)
		n, err := conn.Read(buffer)
		if err != nil {
			log.Fatal("Failed to get reply")
		}

		reply := string(buffer[:n])
		log.Println(reply)

	} else if os.Args[1] == "juice" {
		if len(os.Args) < 6 {
			printUsage()
			return
		}
	}
}

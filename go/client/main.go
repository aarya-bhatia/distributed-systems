package main

import (
	"cs425/common"
	"fmt"
	"net"
	"os"
	"time"
)

var Log = common.Log

func Connect() net.Conn {
	for _, node := range common.Cluster {
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", node.Hostname, node.TCPPort))
		if err == nil {
			return conn
		}
	}

	Log.Fatal("All nodes are offline")
	return nil
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("Upload file: put <local_filename> <remote_filename>")
	fmt.Println("Download file: get <remote_filename> <local_filename>")
	fmt.Println("Query file: query <remote_filename>")
}

func main() {
	if os.Getenv("prod") == "true" {
		common.Cluster = common.ProdCluster
		Log.Info("using prod cluster", common.Cluster)
	} else {
		common.Cluster = common.LocalCluster
		Log.Info("using local cluster", common.Cluster)
	}

	if len(os.Args) == 1 {
		printUsage()
		return
	}

	tokens := os.Args[1:]
	verb := tokens[0]

	if verb == "put" {
		if len(tokens) != 3 {
			printUsage()
			return
		}

		startTime := time.Now().UnixNano()

		if !UploadFile(tokens[1], tokens[2]) {
			Log.Warn("Failed to upload file")
		} else {
			Log.Debug("Success!")
			endTime := time.Now().UnixNano()
			elapsed := endTime - startTime
			fmt.Println("Upload time (sec): ", float64(elapsed)*1e-9)
		}

	} else if verb == "get" {
		if len(tokens) != 3 {
			printUsage()
			return
		}

		startTime := time.Now().UnixNano()

		if !DownloadFile(tokens[2], tokens[1]) {
			Log.Warn("Failed to download file ", tokens[2])
		} else {
			Log.Debug("Success!")
			endTime := time.Now().UnixNano()
			elapsed := endTime - startTime
			fmt.Println("Download time (sec): ", float64(elapsed)*1e-9)
		}
	} else {
		Log.Warn("Unknown command ", verb)
		printUsage()
	}
}

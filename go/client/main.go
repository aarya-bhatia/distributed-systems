package main

import (
	"cs425/common"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

var Log = common.Log

// This helps reuse same TCP connection to download multiple blocks from a replica
// Maps replcia address to connection
var Connections map[string]net.Conn = nil

func getConnection(addr string) net.Conn {
	if Connections == nil {
		Connections = make(map[string]net.Conn)
	}

	if _, ok := Connections[addr]; !ok {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			Log.Warn("Failed to connect to", addr)
			return nil
		}

		Log.Info("Connected to", addr)
		Connections[addr] = conn
	}

	return Connections[addr]
}

func closeConnections() {
	for addr, conn := range Connections {
		Log.Info("Closing connection with", addr)
		conn.Close()
	}
}

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

func deleteFile(filename string) bool {
	server := Connect()
	defer server.Close()

	if !common.SendMessage(server, "DELETE_FILE "+filename) {
		return false
	}

	return common.GetOKMessage(server)
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("Upload file: put <local_filename> <remote_filename>")
	fmt.Println("Download file: get <remote_filename> <local_filename>")
	fmt.Println("Delete file: delete <filename>")
	fmt.Println()
	// fmt.Println("Query file: query <remote_filename>")
}

func main() {
	f, err := os.OpenFile(os.Getenv("HOME") + "/client.log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Printf("error opening file: %v", err)
	}
	defer f.Close()

	Log.DebugLogger.SetOutput(f)
	Log.InfoLogger.SetOutput(f)
	Log.WarnLogger.SetOutput(f)
	Log.FatalLogger.SetOutput(f)

	hostname, err := os.Hostname()
	if err != nil {
		Log.Fatal(err)
	}

	if strings.Index(os.Getenv("env"), "prod") == 0 || (len(hostname) > 0 && strings.Index(hostname, "illinois.edu") > 0) {
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
			fmt.Println("Total Upload time (sec): ", float64(elapsed)*1e-9)
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
			fmt.Println("Total Download time (sec): ", float64(elapsed)*1e-9)
		}

	} else if verb == "delete" {
		if len(tokens) != 2 {
			printUsage()
			return
		}

		if deleteFile(tokens[1]) {
			Log.Debug("Success!")
		}

	} else {
		Log.Warn("Unknown command ", verb)
		printUsage()
	}
}

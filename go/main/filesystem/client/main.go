package main

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/jedib0t/go-pretty/table"
	"github.com/sirupsen/logrus"
)

func printUsage() {
	fmt.Println("Usage: ./client <serverID> <command> <args>...")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("ls <file>")
	fmt.Println("get <remote> <local>")
	fmt.Println("put <local> <remote>")
	fmt.Println("delete <remote>")
	fmt.Println()
}

func main() {
	if len(os.Args) < 3 {
		printUsage()
		return
	}

	hostname, err := os.Hostname()
	if err != nil {
		logrus.Fatal(err)
	}

	if strings.Index(hostname, "illinois.edu") > 0 {
		common.Cluster = common.SDFSProdCluster
	}

	logrus.Println("VM cluster:", common.Cluster)

	ID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		logrus.Fatal(err)
	}
	node := common.GetNodeByID(ID)
	sdfsServer := common.GetAddress(node.Hostname, node.RPCPort)
	logrus.Println("SDFS Server:", sdfsServer)
	sdfsClient := client.NewSDFSClient(sdfsServer)
	tokens := os.Args[2:]

	switch tokens[0] {
	case "put":
		if len(tokens) != 3 {
			printUsage()
			return
		}

		if err := sdfsClient.UploadFile(tokens[1], tokens[2]); err != nil {
			logrus.Fatal(err)
		}

	case "get":
		if len(tokens) != 3 {
			printUsage()
			return
		}

		if err := sdfsClient.DownloadFile(tokens[2], tokens[1]); err != nil {
			logrus.Fatal(err)
		}

	case "delete":
		if len(tokens) != 2 {
			printUsage()
			return
		}

		if err := sdfsClient.DeleteFile(tokens[1]); err != nil {
			logrus.Fatal(err)
		}

	case "ls":
		if len(tokens) != 2 {
			printUsage()
			return
		}

		file, err := sdfsClient.GetFile(tokens[1])
		if err != nil {
			logrus.Fatal(err)
		}

		fmt.Println("File:", file.File.Filename)
		fmt.Println("Size:", file.File.FileSize)
		fmt.Println("Version:", file.File.Version)

		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"NO.", "BLOCK", "SIZE", "REPLICAS"})

		for i, block := range file.Blocks {
			t.AppendRow(table.Row{i, block.Block, block.Size, block.Replicas})
		}

		t.Render()

	case "lsdir":
		if len(tokens) != 2 {
			printUsage()
			return
		}

		files, err := sdfsClient.ListDirectory(tokens[1])
		if err != nil {
			logrus.Fatal(err)
		}

		for _, file := range *files {
			fmt.Println(file)
		}
	}
}

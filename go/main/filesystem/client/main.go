package main

import (
	"cs425/common"
	"cs425/filesystem/client"
	"fmt"
	"github.com/jedib0t/go-pretty/table"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)

func printUsage() {
	fmt.Println("Usage: ./client <serverID> <command> <args>...")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("ls <file>: list file metadata")
	fmt.Println("get <remote> <local>: download remote file as local")
	fmt.Println("put <local> <remote>: upload local file as remote")
	fmt.Println("delete <remote>: delete file")
	fmt.Println("rmdir <prefix>: delete all files matching prefix")
	fmt.Println("cat <prefix> <local>: read and concatenate all files matching prefix")
	fmt.Println()
}

func main() {
	if len(os.Args) < 3 {
		printUsage()
		return
	}

	common.Setup()

	ID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	node := common.GetNodeByID(ID, common.SDFSCluster)
	sdfsServer := common.GetAddress(node.Hostname, node.RPCPort)
	log.Println("SDFS Server:", sdfsServer)
	sdfsClient := client.NewSDFSClient(sdfsServer)
	tokens := os.Args[2:]

	switch tokens[0] {
	case "put":
		if len(tokens) != 3 {
			printUsage()
			return
		}

		reader, err := client.NewFileReader(tokens[1])
		if err != nil {
			log.Fatal(err)
		}
		if err := sdfsClient.WriteFile(reader, tokens[2], common.FILE_TRUNCATE); err != nil {
			log.Fatal(err)
		}

	case "append":
		if len(tokens) != 3 {
			printUsage()
			return
		}

		reader, err := client.NewFileReader(tokens[1])
		if err != nil {
			log.Fatal(err)
		}
		if err := sdfsClient.WriteFile(reader, tokens[2], common.FILE_TRUNCATE); err != nil {
			log.Fatal(err)
		}

	case "get":
		if len(tokens) != 3 {
			printUsage()
			return
		}

		writer, err := client.NewFileWriter(tokens[2])
		if err != nil {
			log.Fatal(err)
		}
		if err := sdfsClient.DownloadFile(writer, tokens[1]); err != nil {
			log.Fatal(err)
		}

	case "cat":
		if len(tokens) != 3 {
			printUsage()
			return
		}

		os.Remove(tokens[2])

		files, err := sdfsClient.ListDirectory(tokens[1])
		if err != nil {
			log.Fatal(err)
		}

		log.Println("files:", *files)

		for _, file := range *files {
			writer, err := client.NewFileWriterWithOpts(tokens[2], os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
			if err != nil {
				log.Fatal(err)
			}

			if err := sdfsClient.DownloadFile(writer, file); err != nil {
				log.Fatal(err)
			}
		}

	case "delete":
		if len(tokens) != 2 {
			printUsage()
			return
		}

		if err := sdfsClient.DeleteFile(tokens[1]); err != nil {
			log.Fatal(err)
		}

	case "rmdir":
		if len(tokens) != 2 {
			printUsage()
			return
		}

		if err := sdfsClient.DeleteAll(tokens[1]); err != nil {
			log.Fatal(err)
		}

	case "ls":
		if len(tokens) != 2 {
			printUsage()
			return
		}

		file, err := sdfsClient.GetFile(tokens[1])
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("File:", file.File.Filename)
		fmt.Println("Size:", file.File.FileSize)

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
			log.Fatal(err)
		}

		for _, file := range *files {
			fmt.Println(file)
		}
	}
}

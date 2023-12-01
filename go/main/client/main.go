package main

import (
	"cs425/common"
	"cs425/filesystem/client"
	"cs425/maplejuice"
	"fmt"
	"github.com/jedib0t/go-pretty/table"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"time"
)

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func printUsage() {
	fmt.Println("Usage: ./client <serverID> <command> <args>...")
	fmt.Println()
	fmt.Println("SDFS Commands:")
	fmt.Println("ls <file>: list file metadata")
	fmt.Println("get <remote> <local>: download remote file as local")
	fmt.Println("put <local> <remote>: upload local file as remote")
	fmt.Println("delete <remote>: delete file")
	fmt.Println("rmdir <prefix>: delete all files matching prefix")
	fmt.Println("cat <prefix> <local>: read and concatenate all files matching prefix")
	fmt.Println()
	fmt.Println("MapleJuice Commands:")
	fmt.Println("maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory> <args...?")
	fmt.Println("juice <juice_exe> <num_juice> <sdfs_intermediate_filename_prefix> <sdfs_dest_file> <args...>")
	os.Exit(1)
}

func main() {
	if len(os.Args) < 3 {
		printUsage()
		return
	}

	common.Setup()

	ID, err := strconv.Atoi(os.Args[1])
	checkError(err)

	node := common.GetNodeByID(ID)
	sdfsServer := common.GetAddress(node.Hostname, node.SDFSRPCPort)
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
		checkError(err)

		err = sdfsClient.WriteFile(reader, tokens[2], common.FILE_TRUNCATE)
		checkError(err)

	case "append":
		if len(tokens) != 3 {
			printUsage()
			return
		}

		reader, err := client.NewFileReader(tokens[1])
		checkError(err)

		err = sdfsClient.WriteFile(reader, tokens[2], common.FILE_TRUNCATE)
		checkError(err)

	case "get":
		if len(tokens) != 3 {
			printUsage()
			return
		}

		writer, err := client.NewFileWriter(tokens[2])
		checkError(err)

		sdfsClient.DownloadFile(writer, tokens[1])
		checkError(err)

	case "cat":
		if len(tokens) != 3 {
			printUsage()
			return
		}

		os.Remove(tokens[2])

		files, err := sdfsClient.ListDirectory(tokens[1])
		checkError(err)

		log.Println("files:", *files)

		for _, file := range *files {
			writer, err := client.NewFileWriterWithOpts(tokens[2], os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
			checkError(err)

			sdfsClient.DownloadFile(writer, file)
			checkError(err)
		}

	case "delete":
		if len(tokens) != 2 {
			printUsage()
			return
		}

		sdfsClient.DeleteFile(tokens[1])
		checkError(err)

	case "rmdir":
		if len(tokens) != 2 {
			printUsage()
			return
		}

		sdfsClient.DeleteAll(tokens[1])
		checkError(err)

	case "ls":
		if len(tokens) != 2 {
			printUsage()
			return
		}

		file, err := sdfsClient.GetFile(tokens[1])
		checkError(err)

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
		checkError(err)

		for _, file := range *files {
			fmt.Println(file)
		}

	case "maple":
		err = Maple(tokens)
		checkError(err)

	case "juice":
		err = Juice(tokens)
		checkError(err)

	default:
		printUsage()
	}
}

func Maple(tokens []string) error {
	conn, err := common.Connect(common.MAPLE_JUICE_LEADER_ID, common.MAPLEJUICE_NODE)
	checkError(err)

	if len(tokens) < 5 {
		printUsage()
	}

	maple_exe := tokens[1]

	num_maples, err := strconv.Atoi(tokens[2])
	checkError(err)

	sdfs_prefix := tokens[3]
	sdfs_src_dir := tokens[4]

	args := tokens[5:]

	param := maplejuice.MapJob{
		ID:           time.Now().UnixNano(),
		MapperExe:    maple_exe,
		NumMapper:    num_maples,
		OutputPrefix: sdfs_prefix,
		InputDir:     sdfs_src_dir,
		Args:         args,
	}

	log.Println("Maple Job:", param)
	return conn.Call(maplejuice.RPC_MAPLE_REQUEST, &param, new(bool))
}

func Juice(tokens []string) error {
	conn, err := common.Connect(common.MAPLE_JUICE_LEADER_ID, common.MAPLEJUICE_NODE)
	checkError(err)

	if len(tokens) < 5 {
		printUsage()
	}

	juice_exe := tokens[1]

	num_juices, err := strconv.Atoi(tokens[2])
	checkError(err)

	sdfs_prefix := tokens[3]
	destfile := tokens[4]

	args := tokens[5:]

	param := maplejuice.ReduceJob{
		ID:          time.Now().UnixNano(),
		ReducerExe:  juice_exe,
		NumReducer:  num_juices,
		InputPrefix: sdfs_prefix,
		OutputFile:  destfile,
		Args:        args,
	}

	log.Println("Juice Job:", param)
	return conn.Call(maplejuice.RPC_JUICE_REQUEST, &param, new(bool))
}

package main

import (
	"cs425/common"
	"cs425/filesystem/client"
	"cs425/maplejuice"
	"fmt"
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"
)

func test() {
	leader := common.SDFSLocalCluster[0]
	addr := common.GetAddress(leader.Hostname, leader.RPCPort)
	log.Println("Connecting to SDFS server:", addr)

	sdfsClient := client.NewSDFSClient(addr)
	writer := client.NewByteWriter()
	if err := sdfsClient.DownloadFile(writer, "hello"); err != nil {
		log.Fatal(err)
	}

	log.Println(writer.String())
}

func printUsage() {
	fmt.Println("maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>")
	os.Exit(1)
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func Maple(tokens []string) error {
	leader := common.MapleJuiceCluster[0]
	conn, err := common.Connect(leader.ID, common.MapleJuiceCluster)
	checkError(err)

	if len(tokens) < 5 {
		printUsage()
	}

	maple_exe := tokens[1]

	num_maples, err := strconv.Atoi(tokens[2])
	checkError(err)

	sdfs_prefix := tokens[3]
	sdfs_src_dir := tokens[4]

	args := maplejuice.MapParam{
		MapperExe:    maple_exe,
		NumMapper:    num_maples,
		OutputPrefix: sdfs_prefix,
		InputDir:     sdfs_src_dir,
	}

	return conn.Call("Leader.MapleRequest", &args, new(bool))
}

func main() {
	common.Setup()

	if len(os.Args) < 2 {
		printUsage()
	}

	tokens := os.Args[1:]

	switch tokens[0] {
	case "maple":
		Maple(tokens)

	default:
		printUsage()
	}
}

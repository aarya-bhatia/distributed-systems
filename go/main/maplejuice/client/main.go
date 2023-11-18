package main

import (
	"cs425/common"
	"cs425/maplejuice"
	"fmt"
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"
)

func printUsage() {
	fmt.Println("maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>")
	fmt.Println("juice <juice_exe> <num_juice> <sdfs_intermediate_filename_prefix> <sdfs_dest_file>")
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

	return conn.Call(maplejuice.RPC_MAPLE_REQUEST, &args, new(bool))
}

func Juice(tokens []string) error {
	leader := common.MapleJuiceCluster[0]
	conn, err := common.Connect(leader.ID, common.MapleJuiceCluster)
	checkError(err)

	if len(tokens) < 5 {
		printUsage()
	}

	juice_exe := tokens[1]

	num_juices, err := strconv.Atoi(tokens[2])
	checkError(err)

	sdfs_prefix := tokens[3]
	destfile := tokens[4]

	args := maplejuice.ReduceParam{
		ReducerExe:  juice_exe,
		NumReducer:  num_juices,
		InputPrefix: sdfs_prefix,
		OutputFile:  destfile,
	}

	return conn.Call(maplejuice.RPC_JUICE_REQUEST, &args, new(bool))
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

	case "juice":
		Juice(tokens)

	default:
		printUsage()
	}
}

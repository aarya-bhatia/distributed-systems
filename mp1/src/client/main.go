package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	// "path/filepath"
	// "time"
)

func main() {
	var args ClientArgs
	var err error

	var reportsDirectory string

	flag.StringVar(&reportsDirectory, "reports", "reports", "The directory to create the report file with current timestamp")

	flag.StringVar(&args.hosts, "hosts", "hosts", "The file containing hosts in the format <id, hostname, port>")
	flag.StringVar(&args.logsDirectory, "logs", "data", "The path containing the log files in format vm{i}.log")
	flag.StringVar(&args.outputDirectory, "outputs", "outputs", "The path to store the output data from the server")
	flag.BoolVar(&args.silence, "silence", false, "To suppress the output of the commands on stdout")
	flag.StringVar(&args.command, "command", "", "The command to execute remotely. Either or 'grep' must be specified.")
	flag.StringVar(&args.grep, "grep", "", "The grep query to execute remotely. Either 'command' or 'grep' must be specified.")

	flag.Parse()

	if args.command == "" && args.grep == "" {
		flag.Usage()
		os.Exit(1)
	}

	if args.command != "" && args.grep != "" {
		flag.Usage()
		os.Exit(1)
	}

	// prepend program name to grep query
	if args.grep != "" {
		args.grep = "grep " + args.grep
	}

	err = exec.Command("mkdir", "-p", args.outputDirectory).Run()

	if err != nil {
		log.Fatal(err)
	}

	err = exec.Command("mkdir", "-p", reportsDirectory).Run()

	if err != nil {
		log.Fatal(err)
	}

	client := RunClient(args)

	// var timestamp string = time.Now().Format("20060102150405")

	// reportFile, err := os.OpenFile(filepath.Join(reportsDirectory, timestamp), os.O_CREATE|os.O_WRONLY, DEFAULT_FILE_MODE)
	//
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// fmt.Println("---Meta data---")
	//
	var count uint = 0

	for _, host := range client.hosts {
		// hostSignature := fmt.Sprintf("%s %s:%s", host.id, host.host, host.port)
		// fmt.Printf("%s, lines: %d, data: %d bytes, latency: %s (%v nanoseconds) \n", hostSignature, host.lines, host.dataSize, host.latency, host.latencyNano)
		// reportFile.Write([]byte(fmt.Sprintf("%s,%s,%s,%d,%v,%d\n", host.id, host.host, host.port, host.lines, host.latencyNano, host.dataSize)))
		if host.status == STATUS_SUCCESS {
			count += 1
		}
	}

	fmt.Printf("Total lines received: %d\n", client.stat.totalLines)
	fmt.Printf("Total hosts successful: %v out of %v\n", count, len(client.hosts))
}

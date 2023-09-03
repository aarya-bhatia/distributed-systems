package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

type Host struct {
	id   string
	host string
	port string
}

func main() {
	hosts := readHosts()

	c := make(chan string)
	argsWithoutProg := strings.Join(os.Args[1:], " ")
	cmd := []byte(argsWithoutProg + "\n")
	fmt.Println("cmd: " + argsWithoutProg)

	for _, host := range hosts {
		fmt.Println(host)
		go connect(host, c, cmd)
	}

	for {
		str := <-c
		fmt.Print(str)
	}
}

func readHosts() []Host {
	hosts := []Host{}
	file, err := os.Open("./hosts")
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
		words := strings.Split(scanner.Text(), " ")
		hosts = append(hosts, Host{words[0], words[1], words[2]})
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println(hosts)
	return hosts
}

func connect(host Host, channel chan string, cmd []byte) {
	conn, err := net.Dial("tcp", host.host+":"+host.port)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Conenct to: " + conn.LocalAddr().String())
	_, err = conn.Write(cmd)
	conn.(*net.TCPConn).CloseWrite()
	connbuf := bufio.NewReader(conn)
	for {
		str, err := connbuf.ReadString('\n')

		if err != nil {
			// log.Fatal(err)
			// fmt.Println(err)
			break
		}
		if str != "\n" {
			channel <- str
		}
	}
}

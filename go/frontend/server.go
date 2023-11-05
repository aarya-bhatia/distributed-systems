package frontend

import (
	"bufio"
	"cs425/common"
	"cs425/filesystem"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"strings"
)

type FrontendServer struct {
	Port          int
	BackendServer *filesystem.Server
}

const MalformedRequestError = "ERROR Malformed Request"
const UnknownRequestError = "ERROR Unknown Request"

func NewServer(info common.Node, backendServer *filesystem.Server) *FrontendServer {
	server := new(FrontendServer)
	server.Port = info.FrontendPort
	server.BackendServer = backendServer
	return server
}

func (server *FrontendServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	buffer, err := reader.ReadString('\n')
	if err != nil {
		return
	}

	if buffer == "\n" || len(buffer) == 0 {
		return
	}

	tokens := strings.Split(buffer[:len(buffer)-1], " ")
	verb := tokens[0]

	// Usage: put <local_filename> <remote_filename>
	if verb == "put" {
		if len(tokens) != 3 {
			common.SendMessage(conn, MalformedRequestError)
			return
		}

		replicas, status := server.uploadFileWithRetry(tokens[1], tokens[2])
		if status != nil {
			log.Warn("Upload failed!")
			common.SendMessage(conn, "UPLOAD_ERROR "+status.Error())
		} else {
			log.Debug("Upload successful!")
			common.SendMessage(conn, "UPLOAD_OK "+strings.Join(replicas, ","))
		}

		// Usage: get <remote_filename> <local_filename>
	} else if verb == "get" {
		if len(tokens) != 3 {
			common.SendMessage(conn, MalformedRequestError)
			return
		}

		if !server.downloadFile(tokens[2], tokens[1]) {
			log.Debug("Download failed!")
			common.SendMessage(conn, "DOWNLOAD_ERROR")
		} else {
			log.Debug("Download success!")
			common.SendMessage(conn, "DOWNLOAD_OK")
		}

		// Usage: delete <remote_filename>
	} else if verb == "delete" {
		if len(tokens) != 2 {
			common.SendMessage(conn, MalformedRequestError)
			return
		}
		if !server.deleteFile(tokens[1]) {
			log.Warn("Delete failed!")
			common.SendMessage(conn, "DELETE_ERROR")
		} else {
			log.Info("Delete successful!")
			common.SendMessage(conn, "DELETE_OK")
		}

	} else {
		common.SendMessage(conn, UnknownRequestError)
		return
	}
}

func (server *FrontendServer) Start() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", server.Port))
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Frontend TCP server is listening on port %d...\n", server.Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go server.handleConnection(conn)
	}
}

// First attempt to connect to SDFS node running locally and get the current
// leader. Then, attempt to connect to the leader node.
func (server *FrontendServer) getLeaderConnection() net.Conn {
	leader := server.BackendServer.GetLeaderNode()
	conn, err := net.Dial("tcp", leader)
	if err != nil {
		log.Warn("Leader is offline:", leader)
		return nil
	}

	log.Info("Connected to leader:", leader)
	return conn
}

// Usage: delete <filename>
func (server *FrontendServer) deleteFile(filename string) bool {
	conn := server.getLeaderConnection()
	if conn == nil {
		return false
	}
	defer conn.Close()

	if !common.SendMessage(conn, "DELETE_FILE "+filename) {
		return false
	}

	return common.GetOKMessage(conn)
}

package main

import (
	"cs425/common"
	"fmt"
	"io"
	"os"
)

func DownloadFile(localFilename string, remoteFilename string) bool {
	server := Connect()
	defer server.Close()

	file, err := os.Create(localFilename)
	if err != nil {
		Log.Warn(err)
		return false
	}

	defer file.Close()
	message := fmt.Sprintf("DOWNLOAD_FILE %s\n", remoteFilename)

	if common.SendAll(server, []byte(message), len(message)) < 0 {
		return false
	}

	if !common.GetOKMessage(server) {
		return false
	}

	buffer := make([]byte, common.BLOCK_SIZE)
	bytesRead := 0

	for {
		n, err := server.Read(buffer)
		if err == io.EOF {
			Log.Debug("EOF")
			break
		} else if err != nil {
			Log.Warn(err)
			return false
		}

		_, err = file.Write(buffer[:n])
		if err != nil {
			Log.Warn(err)
			return false
		}

		bytesRead += n
	}

	Log.Infof("Downloaded file %s with %d bytes\n", localFilename, bytesRead)
	return true
}

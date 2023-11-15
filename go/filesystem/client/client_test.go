package client

import (
	"cs425/common"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"os/exec"
	"testing"
)

func getClient() *SDFSClient {
	common.Cluster = common.SDFSLocalCluster
	node := common.Cluster[0]
	addr := common.GetAddress(node.Hostname, node.RPCPort)
	return NewSDFSClient(addr)
}

func TestClient(t *testing.T) {
	sdfsClient := getClient()

	inputFiles := []string{"data/small", "data/mid", "data/large"}
	remoteFiles := []string{"small", "mid", "large"}
	outputFiles := []string{"small.out", "mid.out", "large.out"}

	for i := range inputFiles {
		reader, err := NewFileReader(inputFiles[i])
		if err != nil {
			log.Println(err)
			t.Fail()
		}

		if err := sdfsClient.WriteFile(reader, remoteFiles[i], common.FILE_TRUNCATE); err != nil {
			log.Println(err)
			t.Fail()
		}

		writer, err := NewFileWriter(outputFiles[i])
		if err != nil {
			log.Println(err)
			t.Fail()
		}

		if err := sdfsClient.DownloadFile(writer, remoteFiles[i]); err != nil {
			log.Println(err)
			t.Fail()
		}

		if err := exec.Command("diff", inputFiles[i], outputFiles[i]).Run(); err != nil {
			log.Println(err)
			t.Fail()
		}
	}
}

func TestAppend(t *testing.T) {
	sdfsClient := getClient()

	assert.Nil(t, sdfsClient.DeleteFile("test"))

	for i := 0; i < 1; i++ {
		assert.Nil(t, sdfsClient.WriteFile(NewByteReader([]byte("Hello\n")), "test", common.FILE_APPEND))
		assert.Nil(t, sdfsClient.WriteFile(NewByteReader([]byte("World\n")), "test", common.FILE_APPEND))
	}

	metadata, err := sdfsClient.GetFile("test")
	assert.Nil(t, err)
	assert.Equal(t, metadata.File.FileSize, 12)

	w := NewByteWriter()
	assert.Nil(t, sdfsClient.DownloadFile(w, "test"))
	assert.Equal(t, w.String(), "Hello\nWorld\n")
}

func TestLargeFile(t *testing.T) {
	sdfsClient := getClient()

	assert.Nil(t, sdfsClient.DeleteFile("large"))

	reader, err := NewFileReader("data/large")
	assert.Nil(t, err)

	err = sdfsClient.WriteFile(reader, "large", common.FILE_TRUNCATE)
	assert.Nil(t, err)

	err = sdfsClient.WriteFile(reader, "large", common.FILE_APPEND)
	assert.Nil(t, err)

	file, err := sdfsClient.GetFile("large")
	assert.Nil(t, err)
	assert.True(t, file.File.FileSize == 2*reader.Size())
	assert.True(t, len(file.Blocks) == common.GetNumFileBlocks(int64(2*reader.Size())))
	assert.True(t, file.Blocks[0].Size == common.BLOCK_SIZE)
	assert.True(t, len(file.Blocks[0].Replicas) > 0)
}

func TestManyWrite(t *testing.T) {
	sdfsClient := getClient()
	filename := "pi"
	data := "0123456789"
	done := make(chan bool)

	assert.Nil(t, sdfsClient.DeleteFile(filename))

	for i := 0; i < 10; i++ {
		go func() {
			assert.Nil(t, sdfsClient.WriteFile(NewByteReader([]byte(data)), filename, common.FILE_TRUNCATE))
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	writer := NewByteWriter()
	assert.Nil(t, sdfsClient.DownloadFile(writer, filename))
	assert.Equal(t, writer.String(), data)
}

func TestDownload(t *testing.T) {
	sdfsClient := getClient()
	inputFilename := "./data/large"
	outputFilename := "oak"

	stat, err := os.Stat(inputFilename)
	assert.Nil(t, err)
	totalSize := int(stat.Size())
	totalBlocks := common.GetNumFileBlocks(stat.Size())

	assert.Nil(t, sdfsClient.DeleteFile(outputFilename))

	reader, err := NewFileReader(inputFilename)
	assert.Nil(t, err)
	assert.Nil(t, sdfsClient.WriteFile(reader, outputFilename, common.FILE_TRUNCATE))

	writer := &TestWriter{}
	assert.Nil(t, sdfsClient.DownloadFile(writer, outputFilename))
	assert.Equal(t, writer.NumBlocks, totalBlocks)
	assert.Equal(t, writer.NumBytes, totalSize)

	elapsed := writer.EndTimeNano - writer.StartTimeNano
	log.Println("Download time:", float64(elapsed)*1e-9)

	// for i := 0; i < 10; i++ {
	// 	go func() {
	// 		writer := NewByteWriter()
	// 		assert.Nil(t, sdfsClient.DownloadFile(writer, filename))
	// 		assert.Equal(t, writer.String(), data)
	// 		done <- true
	// 	}()
	// }
	//
	// for i := 0; i < 10; i++ {
	// 	<-done
	// }
	//
	// endTime = time.Now().UnixNano()
	// elapsedNew := endTime - startTime
	//
	// log.Println("Time 10 reads:", float64(elapsedNew)*1e-9)
}

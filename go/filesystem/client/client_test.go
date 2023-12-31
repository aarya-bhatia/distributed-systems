package client

import (
	"cs425/common"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func init() {
	common.Setup()
}

func getClient() *SDFSClient {
	node := common.Cluster[0]
	addr := common.GetAddress(node.Hostname, node.SDFSRPCPort)
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
}

func TestReadSmallOffsets(t *testing.T) {
	sdfsClient := getClient()
	outputFilename := "hello_world"

	str := "Hello world"

	assert.Nil(t, sdfsClient.DeleteFile(outputFilename))
	assert.Nil(t, sdfsClient.WriteFile(NewByteReader([]byte(str)), outputFilename, common.FILE_TRUNCATE))

	writer := NewByteWriter()

	for i := 0; i < len(str); i++ {
		assert.Nil(t, sdfsClient.ReadFile(writer, outputFilename, i, len(str)-i))
		assert.Equal(t, writer.String(), str[i:])
	}

	for i := 0; i < len(str); i++ {
		assert.Nil(t, sdfsClient.ReadFile(writer, outputFilename, i, 1))
		assert.Equal(t, writer.String(), string(str[i]))
	}
}

func TestReadBigOffsets(t *testing.T) {
	sdfsClient := getClient()
	inputFilename := "./data/vm1"
	outputFilename := "vm1"

	file, err := os.Open(inputFilename)
	assert.Nil(t, err)
	defer file.Close()

	data, err := io.ReadAll(file)
	assert.Nil(t, err)
	dataCopy := make([]byte, len(data))
	assert.Equal(t, copy(dataCopy, data), len(data))

	log.Println("data size:", len(data))

	assert.Nil(t, sdfsClient.DeleteFile(outputFilename))
	assert.Nil(t, sdfsClient.WriteFile(NewByteReader(dataCopy), outputFilename, common.FILE_TRUNCATE))

	writer := NewByteWriter()
	MB := 1024 * 1024
	var offset, length int

	offset = common.BLOCK_SIZE / (2 * MB)
	length = common.BLOCK_SIZE / (4 * MB)
	assert.Nil(t, sdfsClient.ReadFile(writer, outputFilename, offset, length))
	assert.Equal(t, writer.Data, data[offset:offset+length])

	offset = common.BLOCK_SIZE - 512
	length = 1024
	assert.Nil(t, sdfsClient.ReadFile(writer, outputFilename, offset, length))
	assert.Equal(t, len(writer.Data), length)
	assert.Equal(t, string(writer.Data), string(data[offset:offset+length]))
}

func TestManyReads(t *testing.T) {
	count := 100
	sdfsClient := getClient()
	data := "123456"
	assert.Nil(t, sdfsClient.WriteFile(NewByteReader([]byte(data)), "manyreads", common.FILE_TRUNCATE))

	done := make(chan bool)

	for i := 0; i < count; i++ {
		go func() {
			w := NewByteWriter()
			assert.Nil(t, sdfsClient.ReadFile(w, "manyreads", 0, 6))
			assert.Equal(t, w.String(), "123456")
			done <- true
		}()
	}

	for i := 0; i < count; i++ {
		assert.True(t, <-done)
	}
}

func TestManyAppends(t *testing.T) {
	count := 100
	sdfsClient := getClient()

	expectedSum := 0
	done := make(chan bool)

	assert.Nil(t, sdfsClient.DeleteFile("manyappends"))

	for i := 0; i < count; i++ {
		go func(n int) {
			r := NewByteReader([]byte(fmt.Sprintf("%d\n", n)))
			assert.Nil(t, sdfsClient.WriteFile(r, "manyappends", common.FILE_APPEND))
			done <- true
		}(i)
		expectedSum += i
	}

	for i := 0; i < count; i++ {
		assert.True(t, <-done)
	}

	w := NewByteWriter()
	assert.Nil(t, sdfsClient.DownloadFile(w, "manyappends"))
	lines := strings.Split(w.String(), "\n")
	sum := 0
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		num, err := strconv.Atoi(line)
		assert.Nil(t, err)
		sum += num
	}

	assert.Equal(t, sum, expectedSum)
}

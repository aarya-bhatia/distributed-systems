package server

/* import (
	"io"
	"os"

	log "github.com/sirupsen/logrus"
)

type FileCache struct {
	writeCache map[string][]byte
}

func NewFileCache() *FileCache {
	fc := new(FileCache)
	fc.writeCache = make(map[string][]byte)
	return fc
}

func (fc *FileCache) evict() {
}

func (fc *FileCache) loadFile(filename string) bool {
	if fc.hasWriteCache(filename) {
		return true
	}

	f, err := os.Open(filename)
	 i
	if err != nil {
		log.Println(err)
		return false
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		log.Println(err)
		return false
	}

	fc.writeCache[filename] = data
	return true
}

func (fc *FileCache) hasWriteCache(filename string) bool {
	_, ok := fc.writeCache[filename]
	return ok
}

func (fc *FileCache) Write(filename string, data []byte) bool {
	if fc.hasWriteCache(filename) {
		fc.writeCache[filename] = data
	} else {
		fc.loadFile(filename)
	}
}

func (fc *FileCache) Append(filename string, data []byte) bool {
	if !fc.hasWriteCache(filename) {
		if !fc.loadFile(filename) {
			return false
		}
	}
	oldSize := len(fc.writeCache[filename])
	newSize := len(data)
	newData := make([]byte, oldSize+newSize)
	copy(newData[:oldSize], fc.writeCache[filename])
	copy(newData[oldSize:], data)
	fc.writeCache[filename] = newData
	return true
}

func (fc *FileCache) Flush(filename string) bool {
	if !fc.hasWriteCache(filename) {
		return false
	}

	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		log.Println(err)
		return false
	}

	defer f.Close()

	f.Write(fc.writeCache[filename])

	return true
} */

package maplejuice

import (
	log "github.com/sirupsen/logrus"
	"net/rpc"
	"time"
)

type MapTask struct {
	Filename     string
	OffsetLines  int
	CountLines   int
	MapperExe    string
	OutputPrefix string
}

type MapArgs struct {
	Task MapTask
	Data []string
}

func (task MapTask) Start(worker string, data TaskData) bool {
	lines := data.([]string)
	log.Println("Started map task:", len(lines), "lines")

	client, err := rpc.Dial("tcp", worker)
	if err != nil {
		log.Println(err)
		return false
	}
	defer client.Close()

	args := new(MapArgs)
	args.Task = task
	args.Data = data.([]string)

	reply := new(bool)

	if err = client.Call("Service.MapTask", args, reply); err != nil {
		log.Println(err)
		return false
	}

	return *reply
}

func (task MapTask) Restart(worker string) bool {
	time.Sleep(1 * time.Second)
	return true
}

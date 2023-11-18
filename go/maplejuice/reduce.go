package maplejuice

import (
	"cs425/common"
	"fmt"
	"math/rand"
	"net/rpc"

	log "github.com/sirupsen/logrus"
)

// The parameters set by client
type ReduceParam struct {
	NumReducer  int
	ReducerExe  string
	InputPrefix string
	OutputFile  string
}

// A reduce job is a collection of reduce tasks
type ReduceJob struct {
	ID         int64
	InputFiles []string
	Param      ReduceParam
}

// Each reduce task is run by N reducers at a single worker node
type ReduceTask struct {
	ID        int64
	Param     ReduceParam
	InputFile string
}

func (task *ReduceTask) Start(worker int, conn *rpc.Client) bool {
	reply := false
	if err := conn.Call(RPC_REDUCE_TASK, task, &reply); err != nil {
		log.Println(err)
		return false
	}
	return reply
}

func (task *ReduceTask) Hash() int {
	return common.GetHash(fmt.Sprintf("+%v", *task), common.MAX_NODES)
}

func (task *ReduceTask) GetID() int64 {
	return task.ID
}

func (job *ReduceJob) Name() string {
	return fmt.Sprintf("<%d,maple,%s>", job.ID, job.Param.ReducerExe)
}

func (job *ReduceJob) Run(server *Leader) error {
	defer server.Scheduler.Close()

	for _, inputFile := range job.InputFiles {
		log.Println("Input File:", inputFile)

		reduceTask := &ReduceTask{
			ID:        rand.Int63(),
			Param:     job.Param,
			InputFile: inputFile,
		}

		server.Scheduler.AssignTask(reduceTask)
		log.Println("Reduce task scheduled:", reduceTask)
	}
	server.Scheduler.Wait()

	return nil
}

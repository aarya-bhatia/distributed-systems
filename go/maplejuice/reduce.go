package maplejuice

type ReduceJob struct {
	OutputFile string
	NumReducer int
	ReducerExe string
}

type ReduceTask struct {
	Filename    string
	OffsetLines int
	CountLines  int
}

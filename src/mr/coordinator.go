package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int
type TaskType int

var lock sync.Mutex

const (
	Idle TaskState = iota
	InProgress
	Completed
)

const (
	MapTask TaskType = iota
	ReduceTask
)
const (
	Mapping TaskType = iota
	Reducing
	AllDone
)

type Task struct {
	Type   TaskType //任务类型
	taskID int
	state  TaskState

	//map任务
	filename  string
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	Tasks    []Task
	nReduce  int
	State    TaskType
	Mapcount int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	if c.State == AllDone {
		reply.Filename = ""
		return nil
	} else if c.State == Mapping {
		lock.Lock()
		defer lock.Unlock()
		count := 0
		for _, task := range c.Tasks {
			if task.state == Completed {
				count++
			}
		}
		if count == len(c.Tasks) {
			//所有map任务完成，进入reduce阶段

			c.State = Reducing
			//初始化reduce任务
			c.Tasks = []Task{}
			for i := 0; i < c.nReduce; i++ {
				task := Task{
					state:  Idle,
					taskID: i,
					Type:   ReduceTask,
				}
				c.Tasks = append(c.Tasks, task)
			}
			reply.Filename = ""
			reply.NReduce = c.nReduce
			reply.TaskID = 0
			reply.Type = ReduceTask
			reply.Mapcount = c.Mapcount
			c.Tasks[0].state = InProgress
			c.Tasks[0].StartTime = time.Now()
			return nil
		}

		//分配map任务
		for i := range c.Tasks {
			if c.Tasks[i].state == Idle {
				c.Tasks[i].state = InProgress
				c.Tasks[i].StartTime = time.Now()
				reply.Filename = c.Tasks[i].filename
				reply.NReduce = c.nReduce
				reply.TaskID = c.Tasks[i].taskID
				reply.Type = MapTask
				return nil

			}

		}
	} else if c.State == Reducing {
		lock.Lock()
		defer lock.Unlock()
		count := 0
		for _, task := range c.Tasks {
			if task.state == Completed {
				count++
			}
		}
		if count == len(c.Tasks) {
			//所有reduce任务完成，进入done状态
			c.State = AllDone
			reply.Filename = ""
			return nil
		}

		//分配reduce任务
		for i := range c.Tasks {
			if c.Tasks[i].state == Idle {
				c.Tasks[i].state = InProgress
				c.Tasks[i].StartTime = time.Now()
				reply.Filename = "" //reduce任务不需要文件名
				reply.NReduce = c.nReduce
				reply.TaskID = c.Tasks[i].taskID
				reply.Type = ReduceTask
				reply.Mapcount = c.Mapcount
				return nil

			}

		}

	}

	return nil

}

func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	lock.Lock()
	defer lock.Unlock()
	for i := range c.Tasks {
		if c.Tasks[i].taskID == args.TaskID && c.Tasks[i].Type == args.Type {
			c.Tasks[i].state = Completed
			break
		}
	}
	return nil
}
func (c *Coordinator) timeChecker() {
	for {
		time.Sleep(1 * time.Second)
		lock.Lock()
		for _, task := range c.Tasks {
			if task.state == InProgress && time.Since(task.StartTime) > 10*time.Second {
				task.state = Idle
			}
		}
		lock.Unlock()
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		State:   Mapping,
	}
	for index, file := range files {
		task := Task{
			filename: file,
			state:    Idle,
			taskID:   index,
			Type:     MapTask,
		}
		c.Tasks = append(c.Tasks, task)
	}
	c.Mapcount = len(files)

	// Your code here.
	go c.timeChecker()
	c.server()
	return &c
}

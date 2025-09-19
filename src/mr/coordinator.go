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
type CoordinatorState int

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
	Mapping CoordinatorState = iota
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
	MapTasks    []Task
	nReduce     int
	State       CoordinatorState
	ReduceTasks []Task
	Mapcount    int
	Lock        sync.Mutex
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
	switch c.State {
	case AllDone:
		{
			reply.Filename = ""
			return nil
		}
	case Mapping:
		{
			c.Lock.Lock()
			defer c.Lock.Unlock()
			for i := range c.MapTasks {
				if c.MapTasks[i].state == Idle {
					c.MapTasks[i].state = InProgress
					c.MapTasks[i].StartTime = time.Now()
					reply.Filename = c.MapTasks[i].filename
					reply.NReduce = c.nReduce
					reply.TaskID = c.MapTasks[i].taskID
					reply.Type = MapTask
					reply.Mapcount = c.Mapcount
					reply.Success = true
					return nil

				}

			}
			//没有空闲任务，返回空任务
			reply.Success = false
			return nil
		}
	case Reducing:
		{
			c.Lock.Lock()
			defer c.Lock.Unlock()
			for i := range c.ReduceTasks {
				if c.ReduceTasks[i].state == Idle {
					c.ReduceTasks[i].state = InProgress
					c.ReduceTasks[i].StartTime = time.Now()
					reply.Filename = "" //reduce任务不需要文件名
					reply.NReduce = c.nReduce
					reply.TaskID = c.ReduceTasks[i].taskID
					reply.Type = ReduceTask
					reply.Mapcount = c.Mapcount
					reply.Success = true
					return nil

				}

			}
			//没有空闲任务，返回空任务
			reply.Success = false
			return nil
		}
	}
	return nil
}

func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	switch args.Type {
	case MapTask:
		{
			for i := range c.MapTasks {
				if c.MapTasks[i].taskID == args.TaskID && c.MapTasks[i].Type == MapTask {
					c.MapTasks[i].state = Completed
					break
				}
			}
			//检查是否所有map任务完成，若完成则进入reducing阶段
			allDone := true
			for i := range c.MapTasks {
				if c.MapTasks[i].state != Completed {
					allDone = false
					break
				}
			}
			if allDone {
				c.State = Reducing
			}
		}
	case ReduceTask:
		{
			for i := range c.ReduceTasks {
				if c.ReduceTasks[i].taskID == args.TaskID && c.ReduceTasks[i].Type == ReduceTask {
					c.ReduceTasks[i].state = Completed
					break
				}
			}
			//检查是否所有reduce任务完成，若完成则进入alldone阶段
			allDone := true
			for i := range c.ReduceTasks {
				if c.ReduceTasks[i].state != Completed {
					allDone = false
					break
				}
			}
			if allDone {
				c.State = AllDone
			}
		}
	}
	return nil
}

func (c *Coordinator) timeChecker() {
	for {
		time.Sleep(1 * time.Second)
		c.Lock.Lock()
		switch c.State {
		case Mapping:
			{
				for i := range c.MapTasks {
					if c.MapTasks[i].state == InProgress {
						if time.Since(c.MapTasks[i].StartTime) > 10*time.Second {
							//任务超时，重新分配
							c.MapTasks[i].state = Idle
						}
					}
				}
			}
		case Reducing:
			{
				for i := range c.ReduceTasks {
					if c.ReduceTasks[i].state == InProgress {
						if time.Since(c.ReduceTasks[i].StartTime) > 10*time.Second {
							//任务超时，重新分配
							c.ReduceTasks[i].state = Idle
						}
					}
				}
			}
		case AllDone:
			{
				c.Lock.Unlock()

				return
			}
		}
		c.Lock.Unlock()
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
	if c.State == AllDone {
		ret = true
	}
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
		c.MapTasks = append(c.MapTasks, task)
	}
	for i := 0; i < nReduce; i++ {
		task := Task{
			state:  Idle,
			taskID: i,
			Type:   ReduceTask,
		}
		c.ReduceTasks = append(c.ReduceTasks, task)
	}
	c.Mapcount = len(files)
	c.Lock = sync.Mutex{}
	// Your code here.
	go c.timeChecker()
	c.server()
	return &c
}

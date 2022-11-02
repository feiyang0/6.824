package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var mtx sync.Mutex

type Coordinator struct {
	nReduce int
	// 任务发布或者重做
	mapQueue    chan Task
	reduceQueue chan Task
	mInputs     []string
	// 任务屏障
	mapWg    sync.WaitGroup
	reduceWg sync.WaitGroup
	Status   Period

	// 超时时间
	mapDDL    []time.Time
	reduceDDL []time.Time
}

// Your code here -- RPC handlers for the worker to call.
// GetTask :先分配map，map全部执行完后再分配reduce，
// 这里使用waitgroup屏障，使map->reduce任务间执行顺序串行
func (c *Coordinator) getTask(task Task) WorkerReply {
	mtx.Lock()
	defer mtx.Unlock()

	tid := task.GetId()
	reply := WorkerReply{TaskId: tid}
	// set worker ddl
	ddl := time.Now().Add(TaskLimit)
	if c.Status == MapP {
		reply.Type = MapT
		reply.FileName = task.GetInFile()
		reply.FileNum = c.nReduce
		c.mapDDL[tid] = ddl
	} else {
		reply.Type = ReduceT
		reply.FileNum = len(c.mInputs)
		c.reduceDDL[tid] = ddl
	}
	return reply
}
func (c *Coordinator) GetTask(args *WorkerArgs, reply *WorkerReply) error {
	// 对于超时的任务，会想把任务重新加入到任务队列中，并done唤醒wait阻塞
	// 所以wait不阻塞时不一定是全部map任务都完成，需要循环监测
	for c.Status == MapP {
		if len(c.mapQueue) > 0 {
			*reply = c.getTask(<-c.mapQueue)
			// fmt.Printf("[sendTask]: map%d FileName is %s\n", reply.TaskId, reply.FileName)
			return nil
		}
		c.mapWg.Wait()
		if len(c.mapQueue) == 0 { // map任务完成
			c.Status = ReduceP
			c.initReduce()
			break
		}
	}

	// 协程屏障保证map任务全部做完才开始分配reduce任务
	for c.Status == ReduceP {
		if len(c.reduceQueue) > 0 {
			*reply = c.getTask(<-c.reduceQueue)
			return nil
		}
		c.reduceWg.Wait()
		if len(c.reduceQueue) == 0 { // reduce全部完成
			c.Status = FINISH
			break
		}
	}
	// 没有任务，直接返回nil
	return nil
}

func (c *Coordinator) TaskFinish(args *WorkerArgs, reply *WorkerReply) error {
	mtx.Lock()
	defer mtx.Unlock()

	var taskId = args.TaskId
	var taskT = args.Type
	if c.Status == MapP && taskT == MapT &&
		c.mapDDL[taskId] != Boundary { // 这里需要worker和master任务对应（处理超时返回的情况
		c.mapDDL[taskId] = Boundary //DDL设置为固定值Boundary，表示任务完成
		c.mapWg.Done()
		// fmt.Printf("[TaskFin]: map tasks[%d] finish\n", taskId)
	} else if c.Status == ReduceP && taskT == ReduceT &&
		c.reduceDDL[taskId] != Boundary {
		c.reduceDDL[taskId] = Boundary
		c.reduceWg.Done()
		// fmt.Printf("[TaskFin]: reduce tasks[%d] finish\n", taskId)
	}
	return nil
}
func (c *Coordinator) setTime(tid int) {
	mtx.Lock()
	defer mtx.Unlock()
	if c.Status == MapP {
		c.mapDDL[tid] = time.Now().Add(time.Hour * 24)
	}else {
		c.reduceDDL[tid] = time.Now().Add(time.Hour * 24)
	}
}
func (c *Coordinator) pushTask(task Task) {
	if c.Status == MapP {
		// fmt.Printf("[PushTask]: map%d FileName is %s\n",task.GetId(), task.GetInFile())
		c.mapQueue <- task
		c.mapWg.Add(1)
	} else {
		c.reduceQueue <- task
		c.reduceWg.Add(1)
	}
	c.setTime(task.GetId())
}

func (c *Coordinator) initMap() {
	for i, file := range c.mInputs {
		c.pushTask(&MapTask{TaskId: i, infile: file})
	}
}
func (c *Coordinator) initReduce() {
	for i := 0; i < c.nReduce; i++ {
		c.pushTask(&ReduceTask{TaskId: i})
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.Status == FINISH {
		ret = true
	}
	return ret
}

// HandleCrash
// 如果crash则不会调用TaskFinish，少提交一个done会导致GetTask都阻塞在wg.Wait
// 后台开启crash监测
func (c *Coordinator) HandleCrash() {
	// 超时的任务重新入队
	checkTimeout := func(DDL []time.Time) int {
		for tid, ddl := range DDL {
			if ddl.Before(time.Now()) && ddl != Boundary {
				return tid
			}
		}
		return -1
	}
	for {
		time.Sleep(time.Second * 2) // 2s检查一次
		if c.Status == MapP {
			if tid := checkTimeout(c.mapDDL); tid != -1 {
				c.reduceQueue <- &MapTask{infile: c.mInputs[tid], TaskId: tid}
				c.setTime(tid)
				// c.pushTask()
				c.mapWg.Done()
				c.mapWg.Add(1)
				fmt.Printf("[Crash]: map task%d redo\n", tid)
			}
		} else {
			if tid := checkTimeout(c.reduceDDL); tid != -1 {
				c.pushTask(&ReduceTask{TaskId: tid})
				c.reduceWg.Done()
				fmt.Printf("[Crash]: reduce task%d redo\n", tid)
			}
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)

	c := Coordinator{
		mInputs:     files,
		nReduce:     nReduce,
		mapQueue:    make(chan Task, nMap),
		reduceQueue: make(chan Task, nReduce),
		mapDDL:      make([]time.Time, nMap),
		reduceDDL:   make([]time.Time, nReduce),
		Status:      MapP,
	}
	// Your code here.
	c.initMap()
	c.server()
	go c.HandleCrash()
	fmt.Println("server start")
	return &c
}

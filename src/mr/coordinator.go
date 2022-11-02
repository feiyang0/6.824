package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var mtx sync.Mutex

type Coordinator struct {
	nReduce int
	// 任务发布或者重做
	mapQueue    chan Task
	reduceQueue chan Task
	mInputs     []string
	// 记录任务完成情况
	mFinCnt		int32
	rFinCnt		int32

	Status   Period

	// 超时时间
	mapDDL    []time.Time
	reduceDDL []time.Time
}

// Your code here -- RPC handlers for the worker to call.
// GetTask :先分配map，map全部执行完后再分配reduce，
// 这里使用waitgroup屏障，使map->reduce任务间执行顺序串行
func (c *Coordinator) makeReply(task Task) WorkerReply {
	// mtx.Lock() // 是否需要锁?
	// defer mtx.Unlock()
	
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
var once1, once2 sync.Once
func (c *Coordinator) GetTask(args *WorkerArgs, reply *WorkerReply) error {
	
	// 全部map完成后,进入下一个阶段,once.Do对reduce任务初始化一次
	if c.mFinCnt == int32(len(c.mInputs)) { 
		once1.Do(func(){	// 类似waitGroup屏障的作用，切换任务后，关闭管道
			c.Status = ReduceP
			close(c.mapQueue)
			c.initReduce()
			// fmt.Println("[PeriodChange]")
		})
	}
	// 由于crash导致部分任务任务没有完成,
	// 虽然还停留在当前阶段,但是没有任务可取
	// 在没有任务时会休眠等待,
	// 当crash_handler协程重新加入任务时唤醒
	if c.Status == MapP {
		if task, ok := <-c.mapQueue; ok {
			*reply = c.makeReply(task)
			// fmt.Printf("[sendTask]: map%d FileName is %s\n", reply.TaskId, reply.FileName)
			return nil
		}
	}
	// reduce 也要一起退出(reduce parallelism test)
	if c.rFinCnt == int32(c.nReduce) {  
		once2.Do(func(){ 
			c.Status = FINISH
			close(c.reduceQueue)
		})
	}

	if c.Status == ReduceP {
		if task, ok := <-c.reduceQueue; ok {
			*reply = c.makeReply(task)
			// fmt.Printf("[sendTask]: reduce%d\n", reply.TaskId)
			return nil
		}
	}
	if c.Status == FINISH {
		reply.Type = Finish
	}
	// 没有任务，直接返回nil
	return nil
}

func (c *Coordinator) TaskFinish(args *WorkerArgs, reply *WorkerReply) error {
	// mtx.Lock()
	// defer mtx.Unlock()

	var taskId = args.TaskId
	var taskT = args.Type
	if c.Status == MapP && taskT == MapT &&
		c.mapDDL[taskId] != Boundary {  // 这里需要worker和master任务对应（处理超时返回的情况
		
		c.mapDDL[taskId] = Boundary 	//DDL设置为固定值Boundary，表示任务完成
		atomic.AddInt32(&c.mFinCnt, 1)	// 完成一个任务,cnt++
		
		// fmt.Printf("[TaskFin]: map tasks[%d] finish\n", taskId)

	} else if c.Status == ReduceP && taskT == ReduceT &&
		c.reduceDDL[taskId] != Boundary {
		
		c.reduceDDL[taskId] = Boundary
		atomic.AddInt32(&c.rFinCnt, 1)
		
		// fmt.Printf("[TaskFin]: reduce tasks[%d] finish\n", taskId)
	}
	// mtx.Lock()
	
	// defer mtx.Unlock()
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
	} else {
		c.reduceQueue <- task
	}
	c.setTime(task.GetId())
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
		time.Sleep(time.Second * 1) // 2s检查一次
		if c.Status == MapP {
			if tid := checkTimeout(c.mapDDL); tid != -1 {
				c.pushTask(&MapTask{infile: c.mInputs[tid], TaskId: tid})	
				// fmt.Printf("[Crash]: map task%d redo\n", tid)
			}
		} else if c.Status == ReduceP {
			if tid := checkTimeout(c.reduceDDL); tid != -1 {
				c.pushTask(&ReduceTask{TaskId: tid})
				// fmt.Printf("[Crash]: reduce task%d redo\n", tid)
			}
		}	
	}
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
	// fmt.Println("server start")
	return &c
}

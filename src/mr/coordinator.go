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

// var mtx sync.Mutex
type Coordinator struct {
	// 任务发布
	mapQueue    chan Task
	reduceQueue chan Task
	mInputs     []string
	nReduce     int
	// 记录任务完成情况
	mFinCnt		int32
	rFinCnt		int32
	// 当前执行阶段
	Status   Period
	// 超时时间
	mapDDL    []time.Time
	reduceDDL []time.Time
}

// Your code here -- RPC handlers for the worker to call.
// GetTask :先分配map，map全部执行完后再分配reduce，reduce全部执行完才退出程序
// 这里使用once.Do,配合原子操作cnt，实现类似WaitGroup协程屏障
func (c *Coordinator) makeReply(task Task) WorkerReply {
	// mtx.Lock()
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
	
	// 由于crash使部分任务没有提交，或者有些worker取任务时任务已经发送完,
	// 虽然还停留在当前阶段,但是没有任务可取，worker在没有任务时会休眠等待,
	// 当crash_handler协程重新加入任务时唤醒，
	// 或者cnt到达预期值后进入下一个阶段并关闭channel
	if c.Status == MapP {
		if task, ok := <-c.mapQueue; ok {
			*reply = c.makeReply(task)
			// fmt.Printf("[sendTask]: map%d FileName is %s\n", reply.TaskId, reply.FileName)
			return nil
		}
	}
	// 这里不要else if, MapP解除阻塞后，刚好进入ReduceP阶段
	if c.Status == ReduceP {
		if task, ok := <-c.reduceQueue; ok {
			*reply = c.makeReply(task)
			// fmt.Printf("[sendTask]: reduce%d\n", reply.TaskId)
			return nil
		}
	}
	// FINISH，直接返回nil
	return nil
}

func (c *Coordinator) TaskFinish(args *WorkerArgs, reply *WorkerReply) error {
	// mtx.Lock()
	// defer mtx.Unlock()
	// 因为channel的原因taskId在外面不存在并发抢占，任务id相关的资源不用加锁
	var taskId = args.TaskId
	var taskT = args.Type
	if c.Status == MapP && taskT == MapT &&
		c.mapDDL[taskId] != Boundary {  // Task对应master当前phrase，且回复时没有超时
		
		c.mapDDL[taskId] = Boundary 	//DDL设置为固定值Boundary，表示任务完成
		atomic.AddInt32(&c.mFinCnt, 1)	// 完成一个任务,cnt++
		
		// fmt.Printf("[TaskFin]: map tasks[%d] finish\n", taskId)

	} else if c.Status == ReduceP && taskT == ReduceT &&
		c.reduceDDL[taskId] != Boundary {
		
		c.reduceDDL[taskId] = Boundary
		atomic.AddInt32(&c.rFinCnt, 1)
		
		// fmt.Printf("[TaskFin]: reduce tasks[%d] finish\n", taskId)
	}
	// 全部map完成后,进入下一个阶段,once.Do保证任务初始化，close关闭只执行一次
	if c.mFinCnt == int32(len(c.mInputs)) { 
		once1.Do(func(){
			c.Status = ReduceP
			close(c.mapQueue) // 关闭channel解除卡在getTask处的阻塞
			c.initReduce()
			// fmt.Println("[PeriodChange]")
		})
	}
	// reduce 也要一起退出(reduce parallelism test)
	if c.rFinCnt == int32(c.nReduce) {  
		once2.Do(func(){ 
			c.Status = FINISH
			close(c.reduceQueue)
		})
	}
	return nil
}

// 向channel添加任务，并初始化时间
// 这里不用加锁：pushTask在init和后台crash处有使用
// initmap在master开始时调用,initreduce有once.do执行
// crash单协程添加,所以都不存在并发抢占
func (c *Coordinator) pushTask(task Task) {
	tid := task.GetId()
	if c.Status == MapP {
		// fmt.Printf("[PushTask]: map%d FileName is %s\n",task.GetId(), task.GetInFile())
		c.mapQueue <- task
		c.mapDDL[tid] = time.Now().Add(time.Hour * 24)
	} else {
		c.reduceQueue <- task
		c.reduceDDL[tid] = time.Now().Add(time.Hour * 24)
	}
}

// HandleCrash
// 后台开启crash监测，当任务超时重新加入队列，
// 队列有元素后，会唤醒处理call的协程
func (c *Coordinator) HandleCrash() {
	// 取出超时任务，没有返回-1
	checkTimeout := func(DDL []time.Time) int {
		for tid, ddl := range DDL {
			if ddl.Before(time.Now()) && ddl != Boundary {
				return tid
			}
		}
		return -1
	}
	// 超时的任务重新入队
	for {
		time.Sleep(time.Second * 1) // 1s检查一次
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

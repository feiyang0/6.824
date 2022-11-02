package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// TaskType :master当前的状态
type TaskType int

const (
	MapT TaskType = iota
	ReduceT
	Finish
)

var TaskLimit = time.Second * 10
var Boundary = time.UnixMicro(0)

type Period int

const (
	MapP Period = iota
	ReduceP
	FINISH
)

//Add your RPC definitions here.

// WorkerArgs :rpc args
type WorkerArgs struct {
	Type   TaskType
	TaskId int
}

// WorkerReply :rpc reply
type WorkerReply struct {
	Type     TaskType
	TaskId   int
	FileNum  int
	FileName string
}

// Task :对两种任务的抽象worker, master server 都可以用
type Task interface {
	Run()
	GetId() int
	GetType() TaskType
	GetInFile() string
}

type MapTask struct {
	infile  string
	TaskId  int
	nReduce int
	ttype   TaskType
	mapf    func(string, string) []KeyValue
}

func (mt *MapTask) Run() {
	// 将文件内容，读出并存放在content中
	file, err := os.Open(mt.infile)
	if err != nil {
		log.Fatalf("cannot open %v", mt.infile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mt.infile)
	}
	file.Close()

	// 调用mapf取出kv
	kva := mt.mapf(mt.infile, string(content))
	
	// 对kv分类
	nreduce := mt.nReduce
	bucket := make([][]KeyValue, nreduce)
	for _, kv := range kva {
		idx := ihash(kv.Key)%nreduce
		bucket[idx] = append(bucket[idx], kv)
	}
	// 分类完成后写入文件
	for i:=0; i<nreduce; i++ {
		intermediate := fmt.Sprintf("mr-%d-%d", mt.TaskId, i)
		ofile, _ := os.Create(intermediate)
		enc := json.NewEncoder(ofile)
		for _, kv := range bucket[i] {
			err := enc.Encode(kv)
			if err != nil {
				break
			}
		}
		// enc.Encode(&bucket[i]) 得每个kv的encode
		ofile.Close()
	}
}

func (mt *MapTask) GetId() int {
	return mt.TaskId
}
func (mt *MapTask) GetType() TaskType {
	return mt.ttype
}
func (mt *MapTask) GetInFile() string {
	return mt.infile
}

type ReduceTask struct {
	TaskId  int
	nMap    int
	ttype   TaskType
	reducef func(string, []string) string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (rt *ReduceTask) Run() {
	// for file := mr-*-Yid
	kva := []KeyValue{}
	for x := 0; x < rt.nMap; x++ {
		intermediate := fmt.Sprintf("mr-%d-%d", x, rt.TaskId)
		file, err := os.Open(intermediate)
		if err != nil {
			log.Fatalf("cannot open %v", intermediate)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	dir, _ := os.Getwd()
	tfname := fmt.Sprintf("mr-tmp-%d", rt.TaskId)
	tempf, err := ioutil.TempFile(dir,tfname)
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	// oname := fmt.Sprintf("mr-out-%d", rt.TaskId)
	// ofile, _ := os.Create(oname)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := rt.reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempf, "%v %v\n", kva[i].Key, output)
		i = j
	}
	tempf.Close()

	oname := fmt.Sprintf("mr-out-%d", rt.TaskId)
	os.Rename(tempf.Name(), oname)
}
func (rt *ReduceTask) GetId() int {
	return rt.TaskId
}
func (rt *ReduceTask) GetType() TaskType {
	return rt.ttype
}
func (rt *ReduceTask) GetInFile() string {
	return ""
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

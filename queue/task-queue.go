package queue

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/smallnest/rpcx"
	"github.com/smallnest/rpcx/clientselector"
	"github.com/smallnest/rpcx/core"
)

type (
	TaskQueue struct {
		*clientselector.EtcdV3ClientSelector

		server_running map[*core.Client]*taskContext // Client -> Running Task

		task_wait_cnt int
		task_running  []*taskContext // tasks running by a server
		task_done     []*taskContext // tasks completed

		select_list      []reflect.SelectCase
		select_interrupt chan interface{} // interrupt select loop for some reason

		timeout timeout_manager

		sync.Mutex
	}

	TaskParam struct {
		Ctx     context.Context // call context
		Method  string          // call which method
		Args    interface{}     // params for the method
		Reply   interface{}     // used for receiving result
		Timeout time.Duration
	}

	TaskStatus struct {
		Status   TaskState // status of the task,is one of Task_xxx below
		Error    error     // error desc,nil for no error
		CallDone bool      // task done by server(success or fail)
		CtxDone  bool      //task done by client(such as timeout or canceled)
	}

	callContext struct {
		client         *core.Client // client used for this task
		call           *core.Call   // call context
		proxy          *rpcx.Client // proxy for this task
		select_idx     int
		heap_idx       int
		wait_call_done reflect.SelectCase // select case for task done by server
		wait_ctx_done  reflect.SelectCase // select case for task done by client
	}

	taskContext struct {
		notify  chan TaskState
		timeout time.Time
		param   TaskParam
		status  TaskStatus
		callctx callContext
	}

	TaskState int
)

const (
	Task_New TaskState = iota
	Task_Waiting
	Task_Running
	Task_Done
	Task_Timeout
	Task_Aborted
)

var (
	Error_no_server_available = fmt.Errorf("No server available")
	Error_task_timeout        = fmt.Errorf("No server available: Task timeout")
	Error_task_abort          = fmt.Errorf("Task queue exit,task aborted")
)

func NewTaskQueue(s *clientselector.EtcdV3ClientSelector) *TaskQueue {

	ctrl := &TaskQueue{
		EtcdV3ClientSelector: s,
		server_running:       make(map[*core.Client]*taskContext),
		select_interrupt:     make(chan interface{}, 100),
	}

	item := reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctrl.select_interrupt),
	}

	ctrl.select_list = append(ctrl.select_list, item)

	item = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctrl.timeout.NotifyChnl()),
	}

	ctrl.select_list = append(ctrl.select_list, item)

	go ctrl.mainLoop()

	return ctrl
}

func (s *TaskQueue) AddTask(ptr *TaskParam) *taskContext {

	obj := &taskContext{
		param:  *ptr,
		notify: make(chan TaskState, 10),
	}

	if ptr.Timeout > 0 {
		obj.timeout = time.Now().Add(ptr.Timeout)
	}

	obj.status.Status = Task_New

	s.select_interrupt <- obj

	return obj
}

func (s *taskContext) Notify() <-chan TaskState {
	return s.notify
}

func (s *taskContext) Status() TaskStatus {
	return s.status
}

func (s *taskContext) notifyStateChanged() {
	s.notify <- s.status.Status
}

//-----------------------------------------------------------------------

func (s *TaskQueue) mainLoop() {
	for {
		s.select_list[1].Chan = reflect.ValueOf(s.timeout.NotifyChnl())

		idx, recv, ok := reflect.Select(s.select_list)

		if idx == 0 {
			if ok {
				switch val := recv.Interface().(type) {
				case *taskContext:
					s.addNewTask(val)
				case *core.Client:
					delete(s.server_running, val)
				}
			} else {
				return
			}
		} else if idx == 1 {
			s.processTimeout()
		} else if ok {
			s.onTaskCompleted(idx)
			s.runNexTask()
		}
	}
}

func (s *TaskQueue) addNewTask(obj *taskContext) {

	obj.status.Status = Task_Waiting

	obj.notifyStateChanged()

	s.timeout.AddTask(obj)

	if s.task_wait_cnt++; s.task_wait_cnt == 1 {
		s.runNexTask()
	}
}

func (s *TaskQueue) runNexTask() error {
	if s.task_wait_cnt <= 0 {
		return nil
	} else if task := s.timeout.GetNextWatingTask(); task != nil {
		if e := s.runTask(task); e == nil {
			return nil
		} else {
			return e
		}
	} else {
		return nil
	}
}

func (s *TaskQueue) runTask(obj *taskContext) error {

	ptr := &obj.param

	obj.callctx.proxy = rpcx.NewClient(s)

	s.SetClient(obj.callctx.proxy)

	client, err := s.Select(obj.callctx.proxy.ClientCodecFunc)

	if err != nil {
		return err
	}

	s.server_running[client] = obj

	obj.callctx.client = client
	obj.callctx.call = client.Go(ptr.Ctx, ptr.Method, ptr.Args, ptr.Reply, nil)

	obj.callctx.wait_call_done = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(obj.callctx.call.Done),
	}

	obj.callctx.wait_ctx_done = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ptr.Ctx.Done()),
	}

	obj.callctx.select_idx = len(s.select_list)
	s.select_list = append(s.select_list, obj.callctx.wait_call_done)
	s.select_list = append(s.select_list, obj.callctx.wait_ctx_done)

	s.task_running = append(s.task_running, obj)

	obj.status.Status = Task_Running

	obj.notifyStateChanged()

	s.task_wait_cnt--

	return nil
}

func (s *TaskQueue) onTaskCompleted(selectidx int) {

	var taskidx int
	var calldone bool

	/*
		0  select interrupt signal
		1  timeout signal
		2,4,6,... task call done
		3,5,7,... task context done
	*/
	if calldone = (selectidx%2 == 0); calldone {
		taskidx = (selectidx - 2) / 2
	} else {
		taskidx = (selectidx - 3) / 2
	}

	ptask := s.task_running[taskidx]

	if calldone {
		ptask.status.CallDone = true
		ptask.status.Error = ptask.callctx.call.Error
	} else {
		ptask.status.CtxDone = true
		ptask.status.Error = ptask.param.Ctx.Err()
	}

	// Reset timeout
	s.timeout.RemoveTask(ptask)

	s.removeRunningTask(ptask, selectidx, taskidx)

	ptask.status.Status = Task_Done

	ptask.notifyStateChanged()
}

func (s *TaskQueue) removeRunningTask(task *taskContext, selectidx, taskidx int) {

	// remove from running list
	s.task_running = append(s.task_running[:taskidx], s.task_running[taskidx+1:]...)

	for _, item := range s.task_running[taskidx:] {
		item.callctx.select_idx -= 2
	}

	// remove from select_list
	s.select_list = append(s.select_list[:selectidx], s.select_list[selectidx+2:]...)

	// server used by this task is idle now
	delete(s.server_running, task.callctx.client)

	// add to done list
	s.task_done = append(s.task_done, task)
}

//-----------------------------

func (s *TaskQueue) processTimeout() {

	task, old := s.timeout.ProcessTimeout()

	if task == nil {
		return
	}

	switch old {

	case Task_Waiting:

		s.task_wait_cnt--
		s.task_done = append(s.task_done, task)

	case Task_Running:

		selectidx := task.callctx.select_idx
		taskidx := (task.callctx.select_idx - 2) / 2

		s.removeRunningTask(task, selectidx, taskidx)
	}
}

//------------------------------------------------------------------------

func (s *TaskQueue) HandleFailedClient(client *core.Client) {
	s.EtcdV3ClientSelector.HandleFailedClient(client)
	s.select_interrupt <- client
}

func (s *TaskQueue) Select(clientCodecFunc rpcx.ClientCodecFunc, options ...interface{}) (client *core.Client, err error) {

	server_cnt := len(s.EtcdV3ClientSelector.Servers)

	// Try for every server at most.If one server is not running a task,then select it.
	for i := 0; i < server_cnt; i++ {
		if client, err = s.EtcdV3ClientSelector.Select(clientCodecFunc, options...); client != nil {
			if _, ok := s.server_running[client]; !ok {
				return
			}
		} else {
			return
		}
	}

	err = Error_no_server_available

	return
}

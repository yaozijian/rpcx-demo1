package queue

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/smallnest/rpcx"
	"github.com/smallnest/rpcx/clientselector"
	"github.com/smallnest/rpcx/core"
)

type (
	TaskQueue struct {
		*clientselector.EtcdV3ClientSelector

		server_running map[*core.Client]*taskContext // Client -> Running Task

		task_waiting []*taskContext // tasks waiting for process (because of no server available)
		task_running []*taskContext // tasks running by a server
		task_done    []*taskContext // tasks completed

		select_list      []reflect.SelectCase
		select_interrupt chan interface{} // interrupt select loop for some reason
		select_done      *sync.Cond       // select exited
		select_restart   *sync.Cond       // New task added,restart select please

		sync.Mutex
	}

	TaskParam struct {
		Ctx    context.Context // call context
		Method string          // call which method
		Args   interface{}     // params for the method
		Reply  interface{}     // used for receiving result
	}

	TaskStatus struct {
		Status   TaskState // status of the task,is one of Task_xxx below
		Error    error     // error desc,nil for no error
		CallDone bool      // task done by server(success or fail)
		CtxDone  bool      //task done by client(such as timeout or canceled)
	}

	callContext struct {
		client         *core.Client       // client used for this task
		call           *core.Call         // call context
		proxy          *rpcx.Client       // proxy for this task
		select_idx     int                // index in select_list
		wait_call_done reflect.SelectCase // select case for task done by server
		wait_ctx_done  reflect.SelectCase // select case for task done by client
	}

	taskContext struct {
		notify  chan TaskState
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
)

var (
	no_server_available = fmt.Errorf("No server available")
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

	go ctrl.mainLoop()

	return ctrl
}

func (s *TaskQueue) AddTask(ptr *TaskParam) *taskContext {

	obj := &taskContext{
		param:  *ptr,
		notify: make(chan TaskState, 10),
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
		} else if ok {
			s.onTaskCompleted(idx)
			s.runNexTask()
		}
	}
}

func (s *TaskQueue) addNewTask(obj *taskContext) {

	s.task_waiting = append(s.task_waiting, obj)

	obj.status.Status = Task_Waiting

	obj.notifyStateChanged()

	// try to run it if it is first task added
	if len(s.task_waiting) == 1 {
		s.runNexTask()
	}
}

func (s *TaskQueue) runNexTask() error {
	if len(s.task_waiting) > 0 {
		if e := s.runTask(s.task_waiting[0]); e == nil {
			s.task_waiting = append(s.task_waiting[:0], s.task_waiting[1:]...)
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

	s.task_running = append(s.task_running, obj)
	s.select_list = append(s.select_list, obj.callctx.wait_call_done)
	s.select_list = append(s.select_list, obj.callctx.wait_ctx_done)

	obj.status.Status = Task_Running

	obj.notifyStateChanged()

	return nil
}

func (s *TaskQueue) onTaskCompleted(selectidx int) {

	var taskidx int
	var calldone bool

	/*
		0  select interrupt signal
		1,3,5,... task call done
		2,4,6,... task context done
	*/
	if calldone = (selectidx%2 == 1); calldone {
		taskidx = (selectidx - 1) / 2
	} else {
		taskidx = (selectidx - 2) / 2
	}

	// completed task --> task_done
	ptask := s.task_running[taskidx]

	if calldone {
		ptask.status.CallDone = true
		ptask.status.Error = ptask.callctx.call.Error
	} else {
		ptask.status.CtxDone = true
		ptask.status.Error = ptask.param.Ctx.Err()
	}

	s.task_done = append(s.task_done, ptask)
	s.task_running = append(s.task_running[:taskidx], s.task_running[taskidx+1:]...)

	// remove the task from select_list
	s.select_list = append(s.select_list[:selectidx], s.select_list[selectidx+2:]...)

	// server used by this task is idle now
	delete(s.server_running, ptask.callctx.client)

	ptask.status.Status = Task_Done

	ptask.notifyStateChanged()
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

	err = no_server_available

	return
}

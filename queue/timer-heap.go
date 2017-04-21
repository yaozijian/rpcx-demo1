package queue

import (
	"container/heap"
	"time"
)

type (
	task_timeout_heap []*taskContext

	timeout_manager struct {
		timeouts task_timeout_heap
		head     *taskContext
		timer    *time.Timer
		notify   <-chan time.Time
	}
)

func (m *timeout_manager) AddTask(task *taskContext) {
	if heap.Push(&m.timeouts, task); m.head != m.timeouts[0] {
		m.SetupTimer()
	}
}

func (m *timeout_manager) GetNextWatingTask() (task *taskContext) {
	for idx := 0; idx < len(m.timeouts); idx++ {
		if m.timeouts[idx].status.Status == Task_Waiting {
			task = m.timeouts[idx]
			break
		}
	}
	return
}

func (m *timeout_manager) NotifyChnl() <-chan time.Time {
	return m.notify
}

func (m *timeout_manager) ProcessTimeout() (task *taskContext, old TaskState) {
	if m.head != nil {

		task = m.head
		old = task.status.Status

		switch old {
		case Task_Running, Task_Waiting:
			m.TimeoutTask(task)
		}

		m.RemoveTask(task)
	}
	return
}

func (m *timeout_manager) TimeoutTask(task *taskContext) {
	switch task.status.Status {
	case Task_Running:
		task.status.Error = Error_task_running_timeout
	default:
		task.status.Error = Error_task_waiting_timeout
	}
	task.status.Status = Task_Timeout
	task.notifyStateChanged()
}

func (m *timeout_manager) RemoveTask(task *taskContext) {

	heap_idx := task.callctx.heap_idx

	heap.Remove(&m.timeouts, heap_idx)

	if heap_idx == 0 {
		m.SetupTimer()
	}
}

func (m *timeout_manager) stopTimer() {
	if m.timer != nil {
		m.timer.Stop()
	}
	m.notify = nil
	m.head = nil
}

func (m *timeout_manager) SetupTimer() {

	m.stopTimer()

	// setup timer for new nearest timeout task
	if len(m.timeouts) > 0 {
		if task := m.timeouts[0]; !task.timeout.IsZero() {
			if m.timer == nil {
				m.timer = time.NewTimer(task.timeout.Sub(time.Now()))
			} else {
				m.timer.Reset(task.timeout.Sub(time.Now()))
			}
			m.notify = m.timer.C
			m.head = task
		}
	}
}

func (m *timeout_manager) Clear() {

	m.stopTimer()

	// Abort all tasks
	for len(m.timeouts) > 0 {

		task := m.timeouts[0]
		task.status.Status = Task_Aborted
		task.status.Error = Error_task_abort
		task.notifyStateChanged()

		heap.Remove(&m.timeouts, 0)
	}
}

//-------------------------------------------------------------

func (this task_timeout_heap) Len() int {
	return len(this)
}

func (this task_timeout_heap) Less(x, y int) bool {

	var tx, ty int64

	if timeout := this[x].timeout; !timeout.IsZero() {
		tx = timeout.Unix()
	}

	if timeout := this[y].timeout; !timeout.IsZero() {
		ty = timeout.Unix()
	}

	return (ty == 0) || (0 < tx && tx < ty)
}

func (this task_timeout_heap) Swap(x, y int) {
	this[x], this[y] = this[y], this[x]
	this[x].callctx.heap_idx = x
	this[y].callctx.heap_idx = y
}

func (this *task_timeout_heap) Push(v interface{}) {
	task := v.(*taskContext)
	task.callctx.heap_idx = len(*this)
	*this = append(*this, task)
}

func (this *task_timeout_heap) Pop() (v interface{}) {
	if cnt := len(*this); cnt > 0 {
		v = (*this)[cnt-1]
		*this = (*this)[:cnt-1]
	}
	return
}

package queue

import (
	"container/heap"
	"time"
)

type (
	task_timeout_heap []*taskContext

	timeout_manager struct {
		timeouts task_timeout_heap
		timer    *time.Timer
		notify   <-chan time.Time
	}
)

func (m *timeout_manager) AddTask(task *taskContext) {

	var old *taskContext

	if len(m.timeouts) > 0 {
		old = m.timeouts[0]
	}

	heap.Push(&m.timeouts, task)

	if old != m.timeouts[0] {
		if m.stopTimer() {
			m.TimeoutTask(old)
			heap.Remove(&m.timeouts, old.callctx.heap_idx)
		}
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
	if len(m.timeouts) > 0 {

		task = m.timeouts[0]

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
	task.status.Status = Task_Timeout
	task.status.Error = Error_task_timeout
	task.notifyStateChanged()
}

func (m *timeout_manager) RemoveTask(task *taskContext) {

	heap_idx := task.callctx.heap_idx

	heap.Remove(&m.timeouts, task.callctx.heap_idx)

	if heap_idx == 0 {
		m.SetupTimer()
	}
}

func (m *timeout_manager) stopTimer() (r bool) {
	// if there are pending timeout event,read it
	if len(m.notify) > 0 {
		m.timer.Stop()
		<-m.notify
		r = true
	}
	m.notify = nil
	return
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
	tx := this[x]
	ty := this[y]
	tx.callctx.heap_idx, ty.callctx.heap_idx = ty.callctx.heap_idx, tx.callctx.heap_idx
	this[x], this[y] = ty, tx
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

package queue

import (
	"context"
	"time"
)

type (
	ServiceWrapper struct {
		queue *TaskQueue
	}
)

const (
	nested_call_flag = "Nested_Call_Flag"
)

func (w *ServiceWrapper) NeedNestedCall(ctx context.Context) bool {
	return (w.queue != nil) && (&MapContextHelper{ctx}).Get(nested_call_flag) != nested_call_flag
}

func (w *ServiceWrapper) SetTaskQueue(queue *TaskQueue) {
	w.queue = queue
}

func (w *ServiceWrapper) NestedCall(method string, ctx context.Context, args, reply interface{}) (bool, error) {
	if w.NeedNestedCall(ctx) {
		h := &MapContextHelper{ctx}
		h.Header2().Set(nested_call_flag, nested_call_flag)
		return true, w.doNestedCall(h.NewContext(), args, reply, method)
	} else {
		return false, nil
	}
}

func (w *ServiceWrapper) doNestedCall(ctx context.Context, args, reply interface{}, method string) error {

	param := &TaskParam{
		Ctx:    ctx,
		Method: method,
		Args:   args,
		Reply:  reply,
	}

	h := &ContextHelper{ctx}

	if timeout := h.Get("Timeout"); len(timeout) > 0 {
		if dura, e := time.ParseDuration(timeout); e == nil {
			param.Timeout = dura
		}
	}

	if param.Timeout == 0 {
		param.Timeout = 3 * time.Second
	}

	obj := w.queue.AddTask(param)

	for state := range obj.Notify() {
		switch state {
		case Task_Done, Task_Timeout, Task_Aborted:
			return obj.Status().Error
		}
	}

	return nil
}

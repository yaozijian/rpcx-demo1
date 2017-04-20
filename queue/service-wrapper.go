package queue

import (
	"context"
	"reflect"
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
	return (&MapContextHelper{ctx}).Get(nested_call_flag) != nested_call_flag
}

func (w *ServiceWrapper) SetTaskQueue(queue *TaskQueue) {
	w.queue = queue
}

func (w *ServiceWrapper) NestedCall(method string, fnptr interface{}, ctx context.Context, args, reply interface{}) error {

	h := &MapContextHelper{ctx}

	h.Header2().Set(nested_call_flag, nested_call_flag)

	if w.queue == nil {
		argsList := []reflect.Value{
			reflect.ValueOf(h.NewContext()),
			reflect.ValueOf(args),
			reflect.ValueOf(reply),
		}
		retList := reflect.ValueOf(fnptr).Call(argsList)
		e, _ := retList[0].Interface().(error)
		return e
	} else {
		return w.doNestedCall(h.NewContext(), args, reply, method)
	}
}

func (w *ServiceWrapper) doNestedCall(ctx context.Context, args, reply interface{}, method string) error {

	param := &TaskParam{
		Ctx:    ctx,
		Method: method,
		Args:   args,
		Reply:  reply,
	}

	obj := w.queue.AddTask(param)

	for {
		if state := <-obj.Notify(); state == Task_Done {
			return obj.Status().Error
		}
	}
}

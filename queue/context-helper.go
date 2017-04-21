package queue

import (
	"context"

	"github.com/smallnest/rpcx/core"
)

type (
	ContextHelper struct {
		context.Context
	}
)

func (c *ContextHelper) Get(key string) (val string) {
	if h := c.Header(); h != nil {
		val = h.Get(key)
	}
	return
}

func (c *ContextHelper) Header() (h core.Header) {
	h, _ = core.FromContext(c.Context)
	return
}

func (c *ContextHelper) Header2() (h core.Header) {
	var ok bool
	if h, ok = core.FromContext(c.Context); !ok {
		c.Context = core.NewContext(c.Context, make(core.Header))
		h = c.Header()
	}
	return
}

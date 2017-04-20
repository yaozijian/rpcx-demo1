package queue

import (
	"context"
	"net"

	"github.com/smallnest/rpcx/core"
)

type (
	MapContextHelper struct {
		context.Context
	}
)

func (c *MapContextHelper) Get(key string) (val string) {
	if h := c.Header(); h != nil {
		val = h.Get(key)
	}
	return
}

func (c *MapContextHelper) Conn() (conn net.Conn) {
	if m, ok := core.FromMapContext(c.Context); ok {
		conn, _ = m[core.ConnKey].(net.Conn)
	}
	return
}

func (c *MapContextHelper) Header() (h core.Header) {
	if m, ok := core.FromMapContext(c.Context); ok {
		h, _ = m[core.HeaderKey].(core.Header)
	}
	return
}

func (c *MapContextHelper) Header2() (h core.Header) {
	if m, ok := core.FromMapContext(c.Context); ok {
		if h, _ = m[core.HeaderKey].(core.Header); h == nil {
			h = core.NewHeader()
			m[core.HeaderKey] = h
		}
	} else {
		c.Context = core.NewMapContext(c.Context)
		h = c.Header()
	}
	return
}

func (c *MapContextHelper) NewContext() context.Context {
	return core.NewContext(c.Context, c.Header2())
}

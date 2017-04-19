package main

import (
	"context"
	"flag"
	"fmt"
	"rpcx-demo1/common"
	"time"

	"github.com/rcrowley/go-metrics"

	log "github.com/cihub/seelog"
	"github.com/smallnest/rpcx"
	"github.com/smallnest/rpcx/plugin"
)

const (
	console_log = `<seelog>
	<outputs formatid="detail">
		<console/>
	</outputs>
	<formats>
		<format id="detail" format="[%Date(2006-01-02 15:04:05.000)][%File:%Line] %Msg%n" />
	</formats>
</seelog>
`
)

func init() {
	logger, _ := log.LoggerFromConfigAsString(console_log)
	log.ReplaceLogger(logger)
}

type Arith int

func (t *Arith) Mul(ctx context.Context, args *common.Args, reply *common.Reply) error {
	reply.C = args.A * args.B
	return nil
}

var (
	addr = flag.String("s", "127.0.0.1:8972", "service address")
	etcd = flag.String("etcd", "127.0.0.1:2379", "etcd URL")
	n    = flag.String("n", "Arith", "Service name")
)

func main() {

	defer log.Flush()

	flag.Parse()

	etcdplugin := &plugin.EtcdV3RegisterPlugin{
		ServiceAddress:      fmt.Sprintf("tcp@%v", *addr),
		EtcdServers:         []string{*etcd},
		BasePath:            "/rpcx",
		DialTimeout:         3 * time.Second,
		UpdateIntervalInSec: 60,
		Metrics:             metrics.NewRegistry(),
	}

	etcdplugin.Start()

	server := rpcx.NewServer()
	server.PluginContainer.Add(etcdplugin)
	server.PluginContainer.Add(connectionRecorder(0))

	server.RegisterName(*n, new(Arith), "weight=5&state=active")

	server.Serve("tcp", *addr)
}

package main

import (
	"context"
	"flag"
	"fmt"
	"rpcx-demo1/common"
	"rpcx-demo1/queue"
	"time"

	"github.com/rcrowley/go-metrics"

	log "github.com/cihub/seelog"
	"github.com/smallnest/rpcx"
	"github.com/smallnest/rpcx/clientselector"
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

type (
	Arith struct {
		queue.ServiceWrapper
	}
)

func (t *Arith) Mul(ctx context.Context, args *common.Args, reply *common.Reply) error {

	t.SetTaskQueue(task_queue)

	if ok, err := t.NestedCall("Arith.Mul", ctx, args, reply); ok {
		return err
	}

	reply.C = args.A * args.B

	time.Sleep(1000 * time.Millisecond)

	return nil
}

var (
	addr       = flag.String("s", "127.0.0.1:8972", "service address")
	etcd       = flag.String("etcd", "127.0.0.1:2379", "etcd URL")
	n          = flag.String("n", "Arith", "Service name")
	start_ctrl = flag.Bool("ctrl", false, "Start task controller")
	task_queue *queue.TaskQueue
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

	if *start_ctrl {
		startTaskQueue(*etcd, *n)
	}

	server := rpcx.NewServer()
	server.PluginContainer.Add(etcdplugin)
	server.PluginContainer.Add(connectionRecorder(0))

	server.RegisterName(*n, new(Arith), "weight=5&state=active")

	server.Serve("tcp", *addr)
}

//---------------------------------------------------------------

func startTaskQueue(etcd, srvname string) {

	etcd_selector := clientselector.NewEtcdV3ClientSelector(
		[]string{etcd}, "/rpcx/"+srvname,
		time.Minute,
		rpcx.WeightedRoundRobin,
		time.Second*3,
	)

	task_queue = queue.NewTaskQueue(etcd_selector)
}

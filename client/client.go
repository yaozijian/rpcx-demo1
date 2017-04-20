package main

import (
	"context"
	"flag"
	"math/rand"
	"rpcx-demo1/common"
	"sync"
	"time"

	log "github.com/cihub/seelog"
	"github.com/smallnest/rpcx"
	"github.com/smallnest/rpcx/clientselector"
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

var (
	etcd = flag.String("etcd", "127.0.0.1:2379", "etcd URL")
	n    = flag.String("n", "Arith", "Service name")
)

func main() {

	defer log.Flush()

	s := clientselector.NewEtcdV3ClientSelector(
		[]string{*etcd}, "/rpcx/"+*n,
		time.Minute,
		rpcx.WeightedRoundRobin,
		time.Second*3,
	)

	var wait sync.WaitGroup

	rand.Seed(time.Now().Unix())
	client := rpcx.NewClient(s)

	call := func() {
		args := &common.Args{rand.Intn(10), rand.Intn(10)}
		reply := new(common.Reply)

		err := client.Call(context.Background(), *n+".Mul", args, reply)

		if err != nil {
			log.Infof("error for Arith: %d*%d, %v", args.A, args.B, err)
		} else {
			log.Infof("Arith: %d*%d=%d", args.A, args.B, reply.C)
		}
		wait.Done()
	}

	for i := 0; i < 5; i++ {
		wait.Add(1)
		go call()
	}

	wait.Wait()
	client.Close()
}

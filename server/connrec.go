package main

import (
	"net"

	log "github.com/cihub/seelog"
)

type connectionRecorder int

func (this connectionRecorder) Name() string {
	return "ConnectionRecorder"
}

func (this connectionRecorder) HandleConnAccept(conn net.Conn) (net.Conn, bool) {
	log.Infof("client %v --> %v connected", conn.RemoteAddr().String(), conn.LocalAddr().String())
	return conn, true
}

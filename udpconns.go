// Copyright 2017 Mjohh@163.ocm
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package raft

import (
	"errors"
	"fmt"
	"net"
)

type Addr struct {
	Ip   string
	Port int
}

type UdpConns struct {
	myId     int
	addrs    []*Addr
	listener *net.UDPConn
	rcvChan  chan Msg
	sndFail  []int
	rcvFail  []int
	sndOk    []int
	rcvOk    []int
}

func NewUdpConns(addrs []*Addr, myid int) (*UdpConns, error) {
	conns := &UdpConns{addrs: addrs, rcvChan: make(chan Msg, 10)} //TODO
	l := len(addrs)
	if myid >= l || myid < 0 {
		return nil, errors.New("illegal myid val!")
	}
	conns.myId = myid
	conns.sndFail = make([]int, l)
	conns.sndOk = make([]int, l)
	conns.rcvFail = make([]int, l)
	conns.rcvOk = make([]int, l)
	udpaddr := &net.UDPAddr{IP: net.ParseIP(addrs[myid].Ip), Port: addrs[myid].Port}
	listener, err := net.ListenUDP("udp", udpaddr)
	if err != nil {
		return nil, err
	}
	conns.listener = listener
	return conns, nil
}

func (c *UdpConns) Close() {
	c.listener.Close()
}

func (c *UdpConns) Send(dstid int, buf []byte) (n int, err error) {
	if dstid >= len(c.addrs) {
		return 0, errors.New("illegal args!")
	}
	ip := c.addrs[dstid].Ip
	port := c.addrs[dstid].Port

	addr := &net.UDPAddr{IP: net.ParseIP(ip), Port: port}
	n, err = c.listener.WriteToUDP(buf, addr)
	if err != nil {
		c.sndFail[dstid]++
	} else {
		c.sndOk[dstid]++
	}
	return
}

func (c *UdpConns) Broadcast(buf []byte) (n int, err error) {
	errcnt := 0
	for i, _ := range c.addrs {
		if i != c.myId {
			n, err = c.Send(i, buf)
			if err != nil {
				errcnt += 1
			}
		}
	}
	if errcnt > 0 {
		return n, fmt.Errorf("fail times is %d", errcnt)
	}
	return n, nil
}

func (c *UdpConns) Run() {
	for {
		buf := make([]byte, 512)
		n, remoteAddr, err := c.listener.ReadFromUDP(buf)
		//TODO:if listener is closed, need exit the loop ?
		if err != nil {
			continue
		}
		// send to channel
		id, err := addrToId(remoteAddr, c.addrs)
		if err != nil {
			c.rcvFail[id]++
			continue
		}
		c.rcvOk[id]++
		c.rcvChan <- Msg{id, buf[:n]}
	}
}

func (c *UdpConns) RcvChan() <-chan Msg {
	return c.rcvChan
}

func addrToId(addr *net.UDPAddr, addrs []*Addr) (id int, err error) {
	port := addr.Port
	ip := addr.IP.String()

	for i, addr := range addrs {
		if addr.Ip == ip && addr.Port == port {
			return i, nil
		}
	}
	return -1, errors.New("unrecognized remote leader address!")
}

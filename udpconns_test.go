package raft

import (
	"fmt"
	"sync"
	"testing"
	///"time"
)

func nextid(myid int) int {
	if myid == 0 {
		return 1
	} else if myid == 1 {
		return 2
	} else if myid == 2 {
		return 0
	}
	return -1
}

func handle(t *testing.T, myid int, c Conns, wg *sync.WaitGroup) {
	cnt := 0
	for {
		select {
		case <-c.RcvChan():
			//fmt.Printf("\nnode rcv msg = %v", msg)
			///time.Sleep(time.Second)
			//n, err := c.Send(nextid(myid), []byte("ping"))
			c.Send(nextid(myid), []byte("ping"))
			//if err != nil {
			//	fmt.Printf("\nerr=%s", err.Error())
			//}
			//fmt.Printf("\n%d--->%d, n=%d", myid, nextid(myid), n)
			fmt.Printf("\n%d--->%d", myid, nextid(myid))
			//case <-time.After(time.Second * 2):
			//fmt.Printf("\nnode rcv timeout(3s)")
			cnt += 1
			if cnt > 3 {
				wg.Done()
				return
			}
		}
	}
}

func TestConns(t *testing.T) {
	//addrs := []*raft.Addr{&{"127.0.0.1", 2000}, &{"127.0.0.1",2001}, &{"127.0.0.1", 2002}}

	addrs := make([]*Addr, 3)
	addrs[0] = &Addr{"127.0.0.1", 2000}
	addrs[1] = &Addr{"127.0.0.1", 2001}
	addrs[2] = &Addr{"127.0.0.1", 2002}
	udpconns0, err := NewUdpConns(addrs, 0)
	if err != nil {
		t.Errorf("new udpconn 0 fail, and udpconns0=%v!", udpconns0)
	}
	udpconns1, err := NewUdpConns(addrs, 1)
	if err != nil {
		t.Errorf("new udpconn 1 fail, and udpconns1=%v!", udpconns1)
	}
	udpconns2, err := NewUdpConns(addrs, 2)
	if err != nil {
		t.Errorf("new udpconn 2 fail, and udpconns2=%v!", udpconns2)
	}
	var wg sync.WaitGroup
	wg.Add(len(addrs))

	conns := []Conns{udpconns0, udpconns1, udpconns2}
	for i, c := range conns {
		// TODO: handle will quit, but c.Run() keep running, is't routine leak?
		go c.Run()              //run server
		go handle(t, i, c, &wg) //run rcv handler
	}
	// trigger
	////time.Sleep(time.Second)
	//n, err := conns[0].Send(1, []byte("ping"))
	conns[0].Send(1, []byte("ping"))
	//fmt.Printf("\nconns[0].Send, n = %d, err = %v", n, err)
	//select {}
	wg.Wait()
}

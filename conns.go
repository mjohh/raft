package raft

type Msg struct {
	SrcId int
	Buf   []byte
}

// Conns interface define the communications way between eatch nodes
type Conns interface {
	// send to dest node
	Send(dstId int, buf []byte) (n int, err error)

	// send to all other nodes
	Broadcast(buf []byte) (n int, err error)

	// return a chan, and user get msg from the chan
	RcvChan() <-chan Msg

	// run receiving server, which will send msg to the chan
	Run()

	// close connection or socket
	Close()
}

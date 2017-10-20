package raft

import (
	//"fmt"
	"math/rand"
	"time"
)

// int: fixed size for binary.Write/Read
type LogEntry struct {
	Command []byte
	Term    int
}

type raft struct {
	// persistent
	currentTerm int
	votedFor    int
	//log []LogEntry
	log Logger //index start from 1
	// save/restore states
	f Persistence
	// apply log entries to state machine
	applier Applier
	// volatile on all servers
	commitIndex int
	lastApplied int

	// volatile on leaders
	nextIndex  []int
	matchIndex []int

	//// followings are members for protocol state machine implementation
	nodes                 int
	myId                  int
	state                 State
	conns                 Conns
	heartbeatTimeoutCnt   int    //for leader, cnt of KBaseTimeout
	electionTimeoutCnt    int    //for follower & candidate, cnt of KBaseTimeout
	electionTimeoutRandom int    //random init val for election timeout
	voteGranteds          []bool //for candidate and leader
}

// must be exported fields, otherwise binary.Read()
// will panic, for trying writing unexported fields
type AppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesResults struct {
	Term    int
	Success bool
}

type RequestVote struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResults struct {
	Term        int
	VoteGranted bool
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (r *raft) init(myId int, nodes int, log Logger, f Persistence, applier Applier, conns Conns) error {
	var err error
	r.log = log
	r.f = f
	r.applier = applier
	// restore from disk
	if err = f.Restore("currentTerm", &r.currentTerm); err != nil {
		r.currentTerm = 0
	}

	if err = f.Restore("votedFor", &r.votedFor); err != nil {
		// TODO : =-1 ?
		r.votedFor = -1
	}

	// volatile on all servers, start from 1
	r.commitIndex = 0
	r.lastApplied = 0

	// volatile on leaders
	// each node has chance to become leader
	// no used when it's follower or candidater
	r.nodes = nodes
	r.nextIndex = make([]int, nodes)
	r.matchIndex = make([]int, nodes)
	r.voteGranteds = make([]bool, nodes)

	r.myId = myId
	r.state = FollowerState
	r.conns = conns
	r.heartbeatTimeoutCnt = 0                     //for leader, cnt of KBaseTimeout
	r.electionTimeoutCnt = 0                      //for follower & candidater, cnt of KBaseTimeout
	r.electionTimeoutRandom = genRandom(150, 300) //random init val for election timeout

	return err
}

// if error !=nil, call should discard this result, and wait leader retry.
func (r *raft) appendEntriesReceiver(p *AppendEntries) (*AppendEntriesResults, error) {
	if p.Term < r.currentTerm {
		return &AppendEntriesResults{Term: r.currentTerm, Success: false}, nil
	}

	lastIndex, err := r.log.LastIndex()
	if err != nil {
		return nil, err
	}
	if lastIndex < p.PrevLogIndex {
		return &AppendEntriesResults{Term: r.currentTerm, Success: false}, nil
	}
	entries, err := r.log.Read(p.PrevLogIndex, p.PrevLogIndex+1)
	if err != nil {
		return nil, err
	}
	if entries[0].Term != p.PrevLogTerm {
		return &AppendEntriesResults{Term: r.currentTerm, Success: false}, nil
	}
	// 3. If an existing entry conflicts with a new one(same index but different terms),
	// delete the existing entry and all that follow
	// 4. Append any new entries not alredy in the log
	// TODO: just overwrite directly, is it most efficient?
	//r.log = append(r.log[:p.PrevLogIndex], p.Entries...)
	if err := r.log.Write(p.PrevLogIndex+1, p.Entries); err != nil {
		return nil, err
	}

	if p.LeaderCommit > r.commitIndex {
		// attention: the log's last index has been updated
		lastIndex, err := r.log.LastIndex()
		if err != nil {
			return nil, err
		}
		r.commitIndex = min(p.LeaderCommit, lastIndex)
		// TODO:apply the new committed log to state machine
		// and update lastApplied
		entries, err = r.log.Read(r.lastApplied+1, r.commitIndex+1)
		if err != nil {
			return nil, err
		}
		// TODO: need save lastApplied to file as currentTerm and votedFor ?
		r.applier.Apply(entries)
		r.votedFor = p.LeaderId
		r.lastApplied = r.commitIndex
	}
	r.currentTerm = p.Term
	return &AppendEntriesResults{Term: r.currentTerm, Success: true}, nil
}

func (r *raft) requestVoteReceiver(p *RequestVote) (*RequestVoteResults, error) {
	if p.Term < r.currentTerm {
		return &RequestVoteResults{Term: r.currentTerm, VoteGranted: false}, nil
	}
	if r.votedFor != -1 && r.votedFor != p.CandidateId {
		return &RequestVoteResults{Term: r.currentTerm, VoteGranted: false}, nil
	}
	lastIndex, err := r.log.LastIndex()
	if err != nil {
		return nil, err
	}
	var lastTerm int
	if lastIndex == 0 {
		lastTerm = 0
	} else {
		entries, err := r.log.Read(lastIndex, lastIndex+1)
		if err != nil {
			return nil, err
		}
		lastTerm = entries[0].Term
	}
	if p.LastLogIndex >= lastIndex && p.LastLogTerm >= lastTerm {
		r.votedFor = p.CandidateId
		r.currentTerm = p.Term
		return &RequestVoteResults{Term: r.currentTerm, VoteGranted: true}, nil
	}
	return &RequestVoteResults{Term: r.currentTerm, VoteGranted: false}, nil
}

//// raft protocol state machine implement
type State int

const (
	FollowerState State = iota
	CandidateState
	LeaderState
)

const (
	KHeartbeatTimeout = 100 //ms
	KBaseTimeout      = 10  //ms, n (n=KHeartbeatTimeout/KBaseTimeout) base timeout events cause 1 heartbeat timeout event
)

type BaseTimeoutEvt int

//type ElectionTimeoutEvt int
//type HeartbeatTimeoutEvt int
type RPCEvt struct {
	srcId int
	o     interface{}
}

func (r *raft) followerHandler(evt interface{}) {
	switch e := evt.(type) {
	case *RPCEvt:
		switch o := e.o.(type) {
		case *RequestVote:
			r.resetElectionTimeout()
			r.saveStates()
			r.votefor(o)
		case *AppendEntries:
			r.resetElectionTimeout()
			r.saveStates()
			r.appendEntries(o)
		default:
		}
	case BaseTimeoutEvt:
		r.electionTimeoutCnt += 1
		if r.isElectionTimeout() {
			r.currentTerm += 1
			r.votedFor = r.myId
			r.state = CandidateState
			r.resetElectionTimeout()
			r.saveStates()
			r.requestVote()
		}
	default:
	}
}

func (r *raft) candidateHandler(evt interface{}) {
	switch e := evt.(type) {
	case *RPCEvt:
		switch o := e.o.(type) {
		case *RequestVoteResults:
			if e.srcId > -1 && e.srcId < r.nodes {
				r.voteGranteds[e.srcId] = true
				if r.getMajority() {
					r.state = LeaderState
					r.initIndexAryOnLeader()
					r.sndAppendEntries()
				}
			}
		case *AppendEntries:
			if o.Term >= r.currentTerm {
				r.state = FollowerState
				r.cleanVoteGranteds()
				r.cleanIndexAryOnLeader()
				r.resetElectionTimeout()
				r.saveStates()
				r.appendEntries(o)
			}
		default:
		}
	case BaseTimeoutEvt:
		// a new election which has no difference with first time
		r.electionTimeoutCnt += 1
		if r.isElectionTimeout() {
			r.currentTerm += 1
			r.votedFor = r.myId
			r.state = CandidateState
			r.resetElectionTimeout()
			r.saveStates()
			r.requestVote()
		}
	default:
	}
}

// TODO: 1. accept cmd from client and apply it to state machine
func (r *raft) leaderHandler(evt interface{}) {
	switch e := evt.(type) {
	case *RPCEvt:
		switch o := e.o.(type) {
		case *AppendEntriesResults:
			if o.Term > r.currentTerm {
				r.state = FollowerState
				r.votedFor = e.srcId
				r.currentTerm = o.Term
				r.cleanVoteGranteds()
				r.cleanIndexAryOnLeader()
				r.resetElectionTimeout()
				r.resetHeatbeatTimeout()
				r.saveStates()
			} else if !o.Success { //o.Term==r.currentTerm
				if e.srcId > -1 && e.srcId < r.nodes {
					if o.Term == r.currentTerm {
						r.nextIndex[e.srcId] -= 1
					}
				}
				// retry
				r.sndAppendEntries()
			} else {
				if e.srcId > -1 && e.srcId < r.nodes {
					// for we just append on entry per time
					lastIndex, _ := r.log.LastIndex()
					//
					if lastIndex >= r.nextIndex[e.srcId] {
						r.matchIndex[e.srcId] = r.nextIndex[e.srcId]
						r.nextIndex[e.srcId] += 1
						r.updateCommitIndex()
						r.apply()
						// retry
						r.sndAppendEntries()
					}
				}
			}
		case *AppendEntries:
			if o.Term > r.currentTerm {
				// TODO
				r.state = FollowerState
				r.cleanVoteGranteds()
				r.cleanIndexAryOnLeader()
				r.resetElectionTimeout()
				r.resetHeatbeatTimeout()
				r.saveStates()
				r.appendEntries(o)
			}
		case *RequestVote:
			if o.Term > r.currentTerm {
				r.state = FollowerState
				r.cleanVoteGranteds()
				r.cleanIndexAryOnLeader()
				r.resetElectionTimeout()
				r.resetHeatbeatTimeout()
				r.saveStates()
				r.votefor(o)
			}
		default:
		}
	case BaseTimeoutEvt:
		r.heartbeatTimeoutCnt += 1
		if r.isHeatbeatTimeout() {
			r.resetHeatbeatTimeout()
			r.sndAppendEntries()
		}

	default:
	}
}

func (r *raft) main(evt interface{}) {
	switch r.state {
	case FollowerState:
		r.followerHandler(evt)
	case CandidateState:
		r.candidateHandler(evt)
	case LeaderState:
		r.leaderHandler(evt)
	default:
	}
}

func (r *raft) loop() {
	for {
		select {
		case msg := <-r.conns.RcvChan():
			// decode buf, and get event type and data obj
			i, e := Decode(msg.Buf)
			if e != nil {
				// TODO: err cnt
				// TODO: if conns is closed, exit the loop or not?
				continue
			}
			rpc := RPCEvt{msg.SrcId, i}
			r.main(&rpc)
		case <-time.After(time.Millisecond * KBaseTimeout):
			var timeout BaseTimeoutEvt = 1 //val is nouseful
			r.main(timeout)
		}
	}
}

func (r *raft) votefor(o *RequestVote) error {
	result, err := r.requestVoteReceiver(o)
	if err != nil {
		return err
	}
	buf, err := Encode(result)
	if err != nil {
		return err
	}
	_, err = r.conns.Send(o.CandidateId, buf)
	return err
}

func (r *raft) appendEntries(o *AppendEntries) error {
	result, err := r.appendEntriesReceiver(o)
	if err != nil {
		return err
	}
	buf, err := Encode(result)
	if err != nil {
		return err
	}
	_, err = r.conns.Send(o.LeaderId, buf)
	return err
}

func (r *raft) isElectionTimeout() bool {
	return r.electionTimeoutCnt*KBaseTimeout > r.electionTimeoutRandom
}

func (r *raft) isHeatbeatTimeout() bool {
	return r.heartbeatTimeoutCnt*KBaseTimeout > KHeartbeatTimeout
}

func genRandom(min, max int) int {
	rand.Seed(time.Now().Unix())
	n := rand.Intn(max - min)
	n = n + min
	return n
}

func (r *raft) requestVote() error {
	lastIndex, err := r.log.LastIndex()
	if err != nil {
		return err
	}
	entries, err := r.log.Read(lastIndex, lastIndex+1)
	if err != nil {
		return err
	}
	lastTerm := entries[0].Term
	o := RequestVote{Term: r.currentTerm, CandidateId: r.myId, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
	buf, err := Encode(&o)
	if err != nil {
		return err
	}
	_, err = r.conns.Broadcast(buf)
	return err
}

func (r *raft) sndAppendEntries() error {
	var err error

	for i := 0; i < r.nodes; i++ {
		if i == r.myId {
			continue
		}
		a := AppendEntries{
			Term:         r.currentTerm,
			LeaderId:     r.myId,
			PrevLogIndex: r.nextIndex[i] - 1,
			LeaderCommit: r.commitIndex,
		}
		if a.PrevLogIndex >= 1 {
			entries, err := r.log.Read(a.PrevLogIndex, a.PrevLogIndex+1)
			if err != nil {
				return err
			}
			a.PrevLogTerm = entries[0].Term
		} else {
			a.PrevLogTerm = 0
		}
		lastIndex, err := r.log.LastIndex()
		if err != nil {
			return err
		}
		if lastIndex >= r.nextIndex[i] {
			// TODO: len(a.Entries) may be too larg, so may be need limit the
			// lenth of entries sending every time
			// note: incr or decr nextIndex is under leader control
			//a.Entries, err = r.log.Read(r.nextIndex[i], lastIndex+1)
			// here we just append one entry every time, so when get success reply
			// just update nextIndex to nextIndex+1
			a.Entries, err = r.log.Read(r.nextIndex[i], r.nextIndex[i]+1)
			if err != nil {
				return err
			}
		} else {
			a.Entries = nil
		}
		buf, err := Encode(&a)
		if err != nil {
			return err
		}
		_, err = r.conns.Send(i, buf)
	}
	return err
}

func (r *raft) resetElectionTimeout() {
	r.electionTimeoutCnt = 0                      // reset timeout
	r.electionTimeoutRandom = genRandom(150, 300) // reinit timeout random val
}

func (r *raft) resetHeatbeatTimeout() {
	r.heartbeatTimeoutCnt = 0
}

func (r *raft) saveStates() error {
	if err := r.f.Save("currentTerm", r.currentTerm); err != nil {
		return err
	}
	if err := r.f.Save("votedFor", r.votedFor); err != nil {
		return err
	}
	return nil
}

func (r *raft) restoreStates() error {
	var err error
	if err = r.f.Restore("currentTerm", &r.currentTerm); err != nil {
		return err
	}
	if err = r.f.Restore("votedFor", &r.votedFor); err != nil {
		return err
	}
	return nil
}

func (r *raft) cleanVoteGranteds() {
	for i := 0; i < len(r.voteGranteds); i++ {
		r.voteGranteds[i] = false
	}
}

func (r *raft) cleanIndexAryOnLeader() {
	for i := 0; i < len(r.nextIndex); i++ {
		r.nextIndex[i] = 0
	}
	for i := 0; i < len(r.matchIndex); i++ {
		r.matchIndex[i] = 0
	}
}

func (r *raft) initIndexAryOnLeader() {
	for i := 0; i < len(r.nextIndex); i++ {
		lastIndex, _ := r.log.LastIndex()
		r.nextIndex[i] = lastIndex + 1
	}
	for i := 0; i < len(r.matchIndex); i++ {
		r.matchIndex[i] = 0
	}
}

func (r *raft) getMajority() bool {
	n := 0
	for i := 0; i < len(r.voteGranteds); i++ {
		if r.voteGranteds[i] {
			n += 1
		}
	}
	return (n+1)*2 > r.nodes
}

func (r *raft) updateCommitIndex() error {
	lastIndex, err := r.log.LastIndex()
	if err != nil {
		return err
	}
	var n int
	for n = r.commitIndex + 1; n <= lastIndex; n++ {
		cnt := 0
		for i := 0; i < r.nodes; i++ {
			if r.matchIndex[i] >= n {
				cnt += 1
			}
		}
		entries, err := r.log.Read(n, n+1)
		if err != nil {
			return err
		}
		if !((cnt+1)*2 > r.nodes && entries[0].Term == r.currentTerm) {
			break
		}
	}
	if n > r.commitIndex {
		r.commitIndex = n
	}
	return nil
}

func (r *raft) apply() error {
	if r.commitIndex > r.lastApplied {
		entries, err := r.log.Read(r.lastApplied+1, r.commitIndex+1)
		if err != nil {
			return err
		}
		r.applier.Apply(entries)
		r.lastApplied = r.commitIndex
	}
	return nil
}

// TODO err handling
func (r *raft) leaderAppendLogs(entries []LogEntry) {
	lastIndex, _ := r.log.LastIndex()
	r.log.Write(lastIndex+1, entries)
	entries2, _ := r.log.Read(r.lastApplied+1, lastIndex+len(entries)+1)
	r.applier.Apply(entries2)
	r.lastApplied = lastIndex + len(entries)
	//TODO reply to clnt
}

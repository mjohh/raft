package raft

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	//"time"
)

// log stub
////////////////////////////////////////////////////
type Log struct {
	entries []LogEntry
}

// overite from (including) index
func (log *Log) Write(index int, entries []LogEntry) error {
	//fmt.Println("----------------------------")
	//fmt.Println("log.entries=", log.entries)
	//fmt.Println("index, entries=", index, entries)
	log.entries = append(log.entries[:index], entries...)
	//lastindex,_ := log.LastIndex()
	//fmt.Println("after append, log.entries=, len(log.entries)=, Log.LastIndex=", log.entries, len(log.entries), lastindex)
	//fmt.Println("after append, entries=", entries)
	return nil
}

// read entries between [fromIndex, toIndex) from logfile
func (log Log) Read(fromIndex int, toIndex int) ([]LogEntry, error) {
	if fromIndex > len(log.entries)-1 || toIndex > len(log.entries) {
		return nil, fmt.Errorf("index out of range!fromIndex=%d, toIndex=%d", fromIndex, toIndex)
	}
	return log.entries[fromIndex:toIndex], nil
}

// last index of the log, if empty, return 0
func (log Log) LastIndex() (int, error) {
	return len(log.entries) - 1, nil
}

// start from index 1
func newLog() *Log {
	return &Log{entries: make([]LogEntry, 1)}
}

// persistence stub
////////////////////////////////////////////////////
type File struct{}

func (f File) Save(key string, val interface{}) error {
	return nil
}
func (f File) Restore(key string, val interface{}) error {
	return errors.New("Restore has not been implemented!")
}

// applier stub
type App struct{}

func (app App) Apply(entries []LogEntry) error {
	return nil
}

func initRaft(t *testing.T) *raft {

	var R raft

	// prepare conns
	addrs := make([]*Addr, 3)
	addrs[0] = &Addr{"127.0.0.1", 3000}
	addrs[1] = &Addr{"127.0.0.1", 3001}
	addrs[2] = &Addr{"127.0.0.1", 3002}
	udpconns0, err := NewUdpConns(addrs, 0)
	if err != nil {
		t.Errorf("new udpconn 0 fail, and udpconns0=%v!", udpconns0)
	}

	// parepare log
	//var log Log
	//log.entries = make([]LogEntry, 1)
	log := newLog()
	// prepare persistence
	var f File

	// prepare applier
	var app App

	myid := 0
	err = R.init(myid, len(addrs), log, &f, &app, udpconns0)
	//if err != nil {
	//	t.Errorf("init fail")
	//}
	return &R
}

type raftstate struct {
	currentTerm int
	votedFor    int
	commitIndex int
	lastApplied int
}

type AppendEntriesTest struct {
	cmd    AppendEntries
	res    AppendEntriesResults
	state2 raftstate  //expect
	log2   []LogEntry //expect
}

//// Append entries rules
////1. Reply false if term < currentTerm
////2. Reply false if log doesn't contain an entry at prevLogIndex whose term matchs
////3. If an existing entry conficts with a  new one(same index but different terms),delete the existing entry and all that follow it
////4. Append any new entries not already in the log
////5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new enty)
const (
	dontCare = -1
)

var testTbl = []AppendEntriesTest{
	// node boot up, myid:0, append one item log, which has not been commited
	////rule4. Append any new entries not already in the log
	{
		cmd: AppendEntries{
			Term:         1,
			LeaderId:     1,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []LogEntry{{Command: []byte("set 1"), Term: 1}},
			LeaderCommit: 0},
		res: AppendEntriesResults{
			Term:    1,
			Success: true},
		state2: raftstate{
			currentTerm: 1,
			votedFor:    dontCare, //for append entries do not chang this field
			commitIndex: 0,
			lastApplied: 0},
		log2: []LogEntry{{Command: []byte("set 1"), Term: 1}},
	},

	// myid:0, append one item log, which has been commited
	////rule5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new enty)
	// update commitIndex
	{
		cmd: AppendEntries{
			Term:         1,
			LeaderId:     1,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []LogEntry{{Command: []byte("set 1"), Term: 1}},
			LeaderCommit: 1},
		res: AppendEntriesResults{
			Term:    1,
			Success: true},
		state2: raftstate{
			currentTerm: 1,
			votedFor:    dontCare,
			commitIndex: 1,
			lastApplied: 1},
		log2: []LogEntry{{Command: []byte("set 1"), Term: 1}},
	},

	// myid:0, append 2 items log, which has not been commited
	///rule4. Append any new entries not already in the log
	{
		cmd: AppendEntries{
			Term:         1,
			LeaderId:     1,
			PrevLogIndex: 1,
			PrevLogTerm:  1,
			Entries: []LogEntry{{Command: []byte("set 2"), Term: 1},
				{Command: []byte("set 3"), Term: 1}},
			LeaderCommit: 1},
		res: AppendEntriesResults{
			Term:    1,
			Success: true},
		state2: raftstate{
			currentTerm: 1,
			votedFor:    dontCare,
			commitIndex: 1,
			lastApplied: 1},
		log2: []LogEntry{{Command: []byte("set 1"), Term: 1},
			{Command: []byte("set 2"), Term: 1},
			{Command: []byte("set 3"), Term: 1}},
	},
	// rule1. Reply false if term < currentTerm
	{
		cmd: AppendEntries{
			Term:         0, //<currentTerm
			LeaderId:     1,
			PrevLogIndex: 1,
			PrevLogTerm:  1,
			Entries:      []LogEntry{{Command: []byte("set 2"), Term: 1}},
			LeaderCommit: 1},
		res: AppendEntriesResults{
			Term:    1,
			Success: false},
		state2: raftstate{
			currentTerm: 1,
			votedFor:    dontCare,
			commitIndex: 1,
			lastApplied: 1},
		log2: []LogEntry{{Command: []byte("set 1"), Term: 1},
			{Command: []byte("set 2"), Term: 1},
			{Command: []byte("set 3"), Term: 1}},
	},
	// rule2. Reply false if log doesn't contain an entry at prevLogIndex whose term matchs
	{
		cmd: AppendEntries{
			Term:         2, //new term
			LeaderId:     1,
			PrevLogIndex: 1,
			PrevLogTerm:  2, // does not contained
			Entries:      []LogEntry{{Command: []byte("set 2"), Term: 2}},
			LeaderCommit: 1},
		res: AppendEntriesResults{
			Term:    1,
			Success: false},
		state2: raftstate{
			currentTerm: 1,
			votedFor:    dontCare,
			commitIndex: 1,
			lastApplied: 1},
		log2: []LogEntry{{Command: []byte("set 1"), Term: 1},
			{Command: []byte("set 2"), Term: 1},
			{Command: []byte("set 3"), Term: 1}},
	},
	// rule3. If an existing entry conficts with a  new one(same index but different terms),delete the existing entry and all that follow it
	{
		cmd: AppendEntries{
			Term:         2, //new term
			LeaderId:     1,
			PrevLogIndex: 2,                                               // match local log
			PrevLogTerm:  1,                                               // match local log
			Entries:      []LogEntry{{Command: []byte("set 4"), Term: 2}}, //differ term
			LeaderCommit: 1},
		res: AppendEntriesResults{
			Term:    2,
			Success: true},
		state2: raftstate{
			currentTerm: 2,
			votedFor:    dontCare,
			commitIndex: 1,
			lastApplied: 1},
		log2: []LogEntry{{Command: []byte("set 1"), Term: 1},
			{Command: []byte("set 2"), Term: 1},
			{Command: []byte("set 4"), Term: 2}}, //overwrite
	},
	////rule5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	{
		cmd: AppendEntries{
			Term:         2, //new term
			LeaderId:     1,
			PrevLogIndex: 2,                                               // match local log
			PrevLogTerm:  1,                                               // match local log
			Entries:      []LogEntry{{Command: []byte("set 4"), Term: 2}}, //differ term
			LeaderCommit: 4},                                              //>commitIndex and min(LeaderCommit, index of last new netry)=index of last new entry
		res: AppendEntriesResults{
			Term:    2,
			Success: true},
		state2: raftstate{
			currentTerm: 2,
			votedFor:    dontCare,
			commitIndex: 3,  //
			lastApplied: 3}, //
		log2: []LogEntry{{Command: []byte("set 1"), Term: 1},
			{Command: []byte("set 2"), Term: 1},
			{Command: []byte("set 4"), Term: 2}}, //overwrite
	},
}

func TestAppendEntriesResults(t *testing.T) {
	R := initRaft(t)
	for i, v := range testTbl {
		res, err := R.appendEntriesReceiver(&v.cmd)
		if err != nil {
			t.Log("error = %s", err.Error())
		}
		checkAppendEntriesResult(t, res, &v.res, i)
		checkRaftStates(t, R, &v.state2, i)
		checkLogs(t, R, v.log2, i)
	}
	R.conns.Close()
}

func checkAppendEntriesResult(t *testing.T, result *AppendEntriesResults, expect *AppendEntriesResults, idx int) {
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("[%d]unexpected append entries result!", idx)
		t.Log("result=", result, "expect=", expect)
	}
}

func checkRaftStates(t *testing.T, result *raft, expect *raftstate, idx int) {
	if !(expect.commitIndex == result.commitIndex &&
		expect.currentTerm == result.currentTerm &&
		expect.lastApplied == result.lastApplied &&
		(expect.votedFor == result.votedFor || -1 == expect.votedFor)) {
		t.Errorf("[%d]unexpected raft state!", idx)
		t.Logf("commitIndex, currentTerm, lastApplied, votedFor:\n")
		t.Logf("result=%d %d %d %d\n", result.commitIndex, result.currentTerm, result.lastApplied, result.votedFor)
		t.Logf("expect=%d %d %d %d\n", expect.commitIndex, expect.currentTerm, expect.lastApplied, expect.votedFor)
	}
}

func checkLogs(t *testing.T, result *raft, expect []LogEntry, idx int) {
	lastIndex, _ := result.log.LastIndex()
	entries, _ := result.log.Read(1, lastIndex+1)
	if !reflect.DeepEqual(entries, expect) {
		t.Errorf("[%d]unexpected logs!", idx)
		t.Log("result=", entries, "expect=", expect)
	}
}

///////////////////////////////////////////////////////////////////////////////
type RequestVoteTest struct {
	cmd         RequestVote
	res         RequestVoteResults //expect
	expectState raftstate          //expect
}

////1.Reply false if term < currentTerm
////2.If votedFor is null or candidatedId, and candidate's log is at least as up-to-date
//    as receiver's log, grant vote
var testTbl2 = []RequestVoteTest{
	{
		////rule2 If votedFor is null or candidatedId, and candidate's log is at least as up-to-date
		//    as receiver's log, grant vote
		cmd: RequestVote{Term: 1,
			CandidateId:  2, //
			LastLogIndex: 0,
			LastLogTerm:  0,
		},
		res: RequestVoteResults{
			Term:        1,
			VoteGranted: true,
		},
		expectState: raftstate{
			currentTerm: 1,
			votedFor:    2, //
			commitIndex: 0,
			lastApplied: 0,
		}, //
	},
	////rule1.Reply false if term < currentTerm
	{
		cmd: RequestVote{Term: 0, //<1
			CandidateId:  2, //
			LastLogIndex: 0,
			LastLogTerm:  0,
		},
		res: RequestVoteResults{
			Term:        1,
			VoteGranted: false,
		},
		expectState: raftstate{
			currentTerm: 1,
			votedFor:    2, //
			commitIndex: 0,
			lastApplied: 0,
		}, //
	},
	////rule2 If votedFor is null or candidatedId, and candidate's log is at least as up-to-date
	//    as receiver's log, grant vote
	{
		cmd: RequestVote{Term: 2,
			CandidateId:  2,  //>1
			LastLogIndex: -1, //<0
			LastLogTerm:  0,
		},
		res: RequestVoteResults{
			Term:        1,
			VoteGranted: false,
		},
		expectState: raftstate{
			currentTerm: 1,
			votedFor:    2, //
			commitIndex: 0,
			lastApplied: 0,
		}, //
	},
}

func checkRequestVoteResults(t *testing.T, result *RequestVoteResults, expect *RequestVoteResults, index int) {
	if !reflect.DeepEqual(result, expect) {
		t.Errorf("[%d]unexpected request vote result!", index)
		t.Log("result=", result, "expect=", expect)
	}
}

func TestRequestVote(t *testing.T) {
	R := initRaft(t)
	for i, v := range testTbl2 {
		res, err := R.requestVoteReceiver(&v.cmd)
		if err != nil {
			t.Log("error = %s", err.Error())
		}
		checkRequestVoteResults(t, res, &v.res, i)
		checkRaftStates(t, R, &v.expectState, i)
	}
	R.conns.Close()
}

///////////////////////////////////////////////////////////////////////////////

type followerHandlerExpect struct {
	currentTerm        int
	votedFor           int
	commitIndex        int
	lastApplied        int
	state              State
	electionTimeoutCnt int
}
type followerHandlerTest struct {
	evt    interface{} //input
	expect followerHandlerExpect
}

func checkFollowerHandlerResults(t *testing.T, r *raft, expect *followerHandlerExpect, index int) {
	if !((expect.currentTerm == dontCare || r.currentTerm == expect.currentTerm) &&
		(expect.votedFor == dontCare || r.votedFor == expect.votedFor) &&
		(expect.commitIndex == dontCare || r.commitIndex == expect.commitIndex) &&
		(expect.lastApplied == dontCare || r.lastApplied == expect.lastApplied) &&
		(expect.state == dontCare || r.state == expect.state) &&
		(expect.electionTimeoutCnt == dontCare || r.electionTimeoutCnt == expect.electionTimeoutCnt)) {
		t.Errorf("[%d]unexpected follower handler result!", index)
		t.Logf("currentTerm,votedFor,commitIndex,lastApplied,state,electionTimeoutCnt:\n")
		t.Logf("result=%d,%d,%d,%d,%v,%d\n", r.currentTerm, r.votedFor, r.commitIndex, r.lastApplied, r.state, r.electionTimeoutCnt)
		t.Logf("expect=%d,%d,%d,%d,%v,%d\n", expect.currentTerm, expect.votedFor, expect.commitIndex, expect.lastApplied, expect.state, expect.electionTimeoutCnt)
	}
}

var testTbl3 = []followerHandlerTest{
	{
		evt: BaseTimeoutEvt(1),
		expect: followerHandlerExpect{
			currentTerm:        dontCare,
			votedFor:           dontCare,
			commitIndex:        dontCare,
			lastApplied:        dontCare,
			state:              dontCare,
			electionTimeoutCnt: 1,
		},
	},
	{
		evt: BaseTimeoutEvt(1),
		expect: followerHandlerExpect{
			currentTerm:        dontCare,
			votedFor:           dontCare,
			commitIndex:        dontCare,
			lastApplied:        dontCare,
			state:              dontCare,
			electionTimeoutCnt: 2, //increased
		},
	},
	{
		evt: &RPCEvt{
			o: &RequestVote{
				Term:         1,
				CandidateId:  2,
				LastLogIndex: 0,
				LastLogTerm:  0,
			},
			srcId: 2,
		},
		expect: followerHandlerExpect{
			currentTerm:        1,
			votedFor:           2,
			commitIndex:        0,
			lastApplied:        0,
			state:              FollowerState,
			electionTimeoutCnt: 0, //cleaned
		},
	},
	{
		evt: &RPCEvt{
			o: &AppendEntries{
				Term:         1,
				LeaderId:     1, //!=votedFor
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []LogEntry{{Command: []byte("set 1"), Term: 1}},
				LeaderCommit: 0,
			},
			srcId: 1,
		},
		expect: followerHandlerExpect{
			currentTerm:        1,
			votedFor:           2, //
			commitIndex:        0,
			lastApplied:        0,
			state:              FollowerState,
			electionTimeoutCnt: 0, //cleaned
		},
	},

	{
		evt: &RPCEvt{
			o: &AppendEntries{
				Term:         1,
				LeaderId:     2, //==votedFor
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []LogEntry{{Command: []byte("set 1"), Term: 1}},
				LeaderCommit: 0,
			},
			srcId: 2,
		},
		expect: followerHandlerExpect{
			currentTerm:        1,
			votedFor:           2, //
			commitIndex:        0,
			lastApplied:        0,
			state:              FollowerState,
			electionTimeoutCnt: 0, //cleaned
		},
	},

	{
		evt: &RPCEvt{
			o: &AppendEntries{
				Term:         1,
				LeaderId:     2, //==votedFor
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []LogEntry{{Command: []byte("set 1"), Term: 1}},
				LeaderCommit: 1,
			}, //
			srcId: 2,
		},
		expect: followerHandlerExpect{
			currentTerm:        1,
			votedFor:           2, //
			commitIndex:        1, //update
			lastApplied:        1, //update
			state:              FollowerState,
			electionTimeoutCnt: 0, //cleaned
		},
	},
}

func TestFollowerHandler(t *testing.T) {
	R := initRaft(t)
	for i, v := range testTbl3 {
		R.followerHandler(v.evt)
		checkFollowerHandlerResults(t, R, &v.expect, i)
	}
	R.conns.Close()
}

func TestElectionTimeout(t *testing.T) {
	R := initRaft(t)
	time := 0
	n := R.electionTimeoutRandom
	for time < n {
		R.followerHandler(BaseTimeoutEvt(1))
		time += KBaseTimeout
	}
	if !(R.electionTimeoutCnt == 0 && R.state == CandidateState) {
		t.Errorf("unexpected election timeout state!")
		t.Logf("R.electionTimeoutCnt=%d, R.state=%v", R.electionTimeoutCnt, R.state)
	}
}

///////////////////////////////////////////////////////////////////////////////
type candidateHandlerTest struct {
	a int
}

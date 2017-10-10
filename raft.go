package raft

//import (
//	"errors"
//)

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
	log Logger

	// volatile on all servers
	commitIndex int
	lastApplied int

	// volatile on leaders
	nextIndex  []int
	matchIndex []int

	//// followings are members for protocol state machine implementation
	state State
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

// if error !=nil, call should discard this result, and wait leader retry.
func (r *raft) appendEntriesReceiver(p *AppendEntries) (error, *AppendEntriesResults) {
	if p.Term < r.currentTerm {
		return nil, &AppendEntriesResults{Term: r.currentTerm, Success: false}
	}
	err, lastIndex := r.log.LastIndex()
	if err != nil {
		return err, nil
	}
	if lastIndex < p.PrevLogIndex {
		return nil, &AppendEntriesResults{Term: r.currentTerm, Success: false}
	}
	err, entries := r.log.Read(p.PrevLogIndex, p.PrevLogIndex+1)
	if err != nil {
		return err, nil
	}
	if entries[0].Term != p.PrevLogTerm {
		return nil, &AppendEntriesResults{Term: r.currentTerm, Success: false}
	}
	// 3. If an existing entry conflicts with a new one(same index but different terms),
	// delete the existing entry and all that follow
	// 4. Append any new entries not alredy in the log
	// TODO: just overwrite directly, is it most efficient?
	//r.log = append(r.log[:p.PrevLogIndex], p.Entries...)
	if err := r.log.Write(p.PrevLogIndex+1, p.Entries); err != nil {
		return err, nil
	}

	if p.LeaderCommit > r.commitIndex {
		// TODO:apply the new committed log to state machine
		// and update lastApplied
		// attention: the log's last index has been updated

		err, lastIndex := r.log.LastIndex()
		if err != nil {
			return err, nil
		}
		r.commitIndex = min(p.LeaderCommit, lastIndex)
	}
	return nil, &AppendEntriesResults{Term: r.currentTerm, Success: true}
}

func (r *raft) requestVoteReceiver(p *RequestVote) (error, *RequestVoteResults) {
	if p.Term < r.currentTerm {
		return nil, &RequestVoteResults{Term: r.currentTerm, VoteGranted: false}
	}
	if r.votedFor != -1 && r.votedFor != p.CandidateId {
		return nil, &RequestVoteResults{Term: r.currentTerm, VoteGranted: false}
	}
	err, lastIndex := r.log.LastIndex()
	if err != nil {
		return err, nil
	}
	if p.LastLogIndex >= lastIndex && p.LastLogTerm >= r.currentTerm {
		r.votedFor = p.CandidateId
		r.currentTerm = p.Term
		return nil, &RequestVoteResults{Term: r.currentTerm, VoteGranted: true}
	}
	return nil, &RequestVoteResults{Term: r.currentTerm, VoteGranted: false}
}

type Logger interface {
	// overwrite entries start at index (include index) to the end of logfile
	Write(index int, entries []LogEntry) error

	// read entries between [fromIndex, toIndex) from logfile
	Read(fromIndex int, toIndex int) (error, []LogEntry)

	// last index of the log, if empty, return -1.
	LastIndex() (error, int)
}

//// raft protocol state machine implement
type State int

const (
	FollowerState State = iota
	CandidateState
	LeaderState
)

type Event int

const (
	//KDontHearFromLeader = 0
	BaseTimeout     Event = iota // n x BaseTimeout = ElectionTimeout or HeartbeatTimeout
	ElectionTimeout              // randomized to be 150ms to 300ms, follower waits until becomming a candidate
	HeartbeatTimeout
	RequestVoteMsg
	AppendEntriesMsg
	RequestVoteResultMsg
	AppendEntriesResultMsg
)

const (
	KHeartbeatTimeout = 100 //ms
	KBaseTimeout      = 10  //ms, n (n=KHeartbeatTimeout/KBaseTimeout) base timeout events cause 1 heartbeat timeout event
)

type Input struct {
	evt  Event
	data interface{}
}

func (r *raft) followerStateHandler(i *Input) {
	if i.evt == ElectionTimeout {
		r.state = CandidateState
		r.currentTerm += 1
		//TODO r.requestVotes()
		//TODO r.resetElectionTimeout()
	} else if i.evt == RequestVoteMsg {
		//r.handleVoteRequest(c)
	} else if i.evt == AppendEntriesMsg {

	}
}

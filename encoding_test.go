package raft

import (
	//"fmt"
	//"github.com/mjohh/raft"
	"reflect"
	"testing"
)

type item struct {
	//t msgtype
	o interface{}
}

func TestEncode(t *testing.T) {

	table := []item{
		{&RequestVote{Term: 1, CandidateId: 1, LastLogIndex: 1, LastLogTerm: 1}},
		{&RequestVoteResults{Term: 2, VoteGranted: true}},
		{&AppendEntries{Term: 3, LeaderId: 3, PrevLogIndex: 3, Entries: []LogEntry{{Command: []byte("set 1"), Term: 2}, {Command: []byte("get 2"), Term: 2}}, LeaderCommit: 3}},
		{&AppendEntriesResults{Term: 3, Success: true}},
	}
	for _, v := range table {

		buf, err := Encode(v.o)
		if err != nil {
			t.Errorf("Encode fail, err=%s", err.Error())
		}
		o2, err := Decode(buf)
		if err != nil {
			t.Errorf("Decode fail, err=%s", err.Error())
		}
		if !reflect.DeepEqual(v.o, o2) {
			t.Errorf("v.o != o2")
			t.Errorf("%v", v.o)
			t.Errorf("%v", o2)
		}
	}
}

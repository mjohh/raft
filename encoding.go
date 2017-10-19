package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
)

const KMagic int32 = 0x19780809

type msgtype byte

const (
	KRequestVoteTag msgtype = iota
	KRequestVoteResultTag
	KAppendEntriesTag
	KAppendEntriesResultTag
)

type msghead struct {
	Magic int32
	Type  msgtype
}

func Encode(i interface{}) ([]byte, error) {
	var err error
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	h := msghead{Magic: KMagic}

	switch i.(type) {
	case *RequestVote:
		h.Type = KRequestVoteTag
	case *RequestVoteResults:
		h.Type = KRequestVoteResultTag
	case *AppendEntries:
		h.Type = KAppendEntriesTag
	case *AppendEntriesResults:
		h.Type = KAppendEntriesResultTag
	default:
		return nil, errors.New("unknow struct type in Encode!")
	}
	if err = enc.Encode(h); err != nil {
		return nil, err
	}
	if err = enc.Encode(i); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(b []byte) (i interface{}, err error) {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	var h msghead
	if err := dec.Decode(&h); err != nil {
		return nil, err
	}
	if h.Magic != KMagic {
		return nil, errors.New("Magic checking fail!")
	}
	switch h.Type {
	case KRequestVoteTag:
		i = new(RequestVote)
	case KRequestVoteResultTag:
		i = new(RequestVoteResults)
	case KAppendEntriesTag:
		i = new(AppendEntries)
	case KAppendEntriesResultTag:
		i = new(AppendEntriesResults)
	default:
		return nil, errors.New("Unknow msg type in Decode!")

	}
	err = dec.Decode(i)
	return i, err
}

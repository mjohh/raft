package raft

//// index start from 1
type Logger interface {
	// overwrite entries start at index (include index) to the end of logfile
	Write(index int, entries []LogEntry) error

	// read entries between [fromIndex, toIndex) from logfile, toIndex==-1: to the end
	Read(fromIndex int, toIndex int) ([]LogEntry, error)

	// last index of the log, if empty, return -1.
	LastIndex() (int, error)
}

type Persistence interface {
	Save(key string, val interface{}) error
	Restore(key string, val interface{}) error
}

type Applier interface {
	//apply log entries to state machine
	Apply(entries []LogEntry) error
}

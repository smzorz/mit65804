package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Tversion uint64

type PutArgs struct {
	Key      string
	Value    string
	Version  Tversion
	SeqNum   int64
	ClientId int64
}

type PutReply struct {
	Err     Err
	Version Tversion
}

type GetArgs struct {
	Key      string
	SeqNum   int64
	ClientId int64
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}

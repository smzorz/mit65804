package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logs        []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	heartTimeDeadline time.Time
	electTimeDeadline time.Time
	state             State
	applyCond         *sync.Cond

	//3D
	lastIncludedIndex int
	lastIncludedTerm  int
	applyCh           chan raftapi.ApplyMsg
}

type Log struct {
	Command interface{} //
	Term    int         // term when entry was received by leader
}
type State int

const (
	Follower  State = 0
	Candidate State = 1
	Leader    State = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	//3D
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log
	//3D
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		//3D
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		//error
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	cutIndex := index - rf.lastIncludedIndex
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logs[cutIndex].Term

	newLogs := make([]Log, len(rf.logs)-cutIndex)
	copy(newLogs, rf.logs[cutIndex:])
	rf.logs = newLogs
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	//3D
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	//3c
	//ConflictIndex int
	//ConflictTerm  int
	XTerm  int
	XIndex int
	XLen   int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.persist()
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	// candidate's log is at least as up-to-date as receiver's log
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.logs[len(rf.logs)-1].Term || (args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= len(rf.logs)-1+rf.lastIncludedIndex) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.electTimeDeadline = time.Now().Add(time.Duration(300+(rand.Int63()%50)) * time.Millisecond)
			rf.persist()
			return
		}
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	rf.persist()
	return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false

		return
	}
	// reset election timeout on any AppendEntries from current leader
	ms := 300 + (rand.Int63() % 50)
	rf.electTimeDeadline = time.Now().Add(time.Duration(ms) * time.Millisecond)
	rf.state = Follower
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XIndex = rf.lastIncludedIndex + 1
		reply.XTerm = -1
		reply.XLen = -1

		return
	}
	myLastLogIndex := len(rf.logs) - 1 + rf.lastIncludedIndex
	if (args.PrevLogIndex > myLastLogIndex) ||
		rf.logs[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		if args.PrevLogIndex > myLastLogIndex {
			reply.XLen = len(rf.logs) + rf.lastIncludedIndex
			reply.XTerm = -1
			reply.XIndex = -1
		} else {
			reply.XLen = -1
			reply.XTerm = rf.logs[args.PrevLogIndex-rf.lastIncludedIndex].Term
			//find first index of XTerm
			xIndex := args.PrevLogIndex
			for xIndex > rf.lastIncludedIndex && rf.logs[xIndex-1-rf.lastIncludedIndex].Term == reply.XTerm {
				xIndex -= 1
			}
			reply.XIndex = xIndex
		}
		rf.persist()
		return
	}
	// If there are entries, append them (handling conflicts). If heartbeat (no entries), skip.
	if args.Entries != nil && len(args.Entries) > 0 {

		for i, entry := range args.Entries {
			currentEntryIndex := args.PrevLogIndex + 1 + i
			currentSliceIndex := currentEntryIndex - rf.lastIncludedIndex
			if currentEntryIndex > myLastLogIndex {
				rf.logs = append(rf.logs, args.Entries[i:]...)
				rf.persist()
				break
			}
			if currentSliceIndex >= 0 && currentSliceIndex < len(rf.logs) {
				if rf.logs[currentSliceIndex].Term != entry.Term {
					rf.logs = rf.logs[:currentSliceIndex]
					rf.logs = append(rf.logs, args.Entries[i:]...)
					rf.persist()
					break
				}
			}
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		newLastLogIndex := len(rf.logs) - 1 + rf.lastIncludedIndex
		oldCommit := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, newLastLogIndex)
		if rf.commitIndex > oldCommit {
			rf.applyCond.Signal()
		}

	}
	rf.persist()
	reply.Term = rf.currentTerm
	reply.Success = true
	return

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	if rf.state != Leader {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}
	newLog := Log{Command: command, Term: rf.currentTerm}
	rf.logs = append(rf.logs, newLog)
	index = len(rf.logs) - 1 + rf.lastIncludedIndex
	term = rf.currentTerm
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.persist()
	rf.mu.Unlock()
	go rf.runAppendEntries()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()

		if rf.state != Leader && time.Now().After(rf.electTimeDeadline) {
			rf.state = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.persist()
			rf.electTimeDeadline = time.Now().Add(time.Duration(300+(rand.Int63()%50)) * time.Millisecond)
			// initialize vote count
			// send RequestVote RPCs to all other servers
			args := &RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.logs) - 1 + rf.lastIncludedIndex
			if args.LastLogIndex >= 0 {
				args.LastLogTerm = rf.logs[args.LastLogIndex-rf.lastIncludedIndex].Term
			} else {
				args.LastLogTerm = 0
			}
			rf.mu.Unlock()
			go rf.runElection(args)
			continue
		} else if rf.state == Leader && time.Now().After(rf.heartTimeDeadline) {
			rf.heartTimeDeadline = time.Now().Add(100 * time.Millisecond)
			rf.mu.Unlock()
			go rf.runAppendEntries()
			continue
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)

	}
}

func (rf *Raft) runElection(args *RequestVoteArgs) {
	voteReceived := 1
	majority := len(rf.peers)/2 + 1
	type voteResult struct {
		Reply   *RequestVoteReply
		Success bool
	}
	voteCh := make(chan voteResult, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(server, args, reply)
				voteCh <- voteResult{Reply: reply, Success: ok}
			}(i)

		}
	}
	for i := 0; i < len(rf.peers)-1; i++ {
		result := <-voteCh
		rf.mu.Lock()
		if result.Success && result.Reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = result.Reply.Term
			rf.votedFor = -1
			rf.mu.Unlock()
			return
		}
		if result.Success && result.Reply.VoteGranted {
			voteReceived += 1
			if voteReceived >= majority && rf.state == Candidate && rf.currentTerm == args.Term {
				rf.state = Leader
				// Initialize nextIndex and matchIndex for each follower
				lastLogIndex := len(rf.logs) - 1 + rf.lastIncludedIndex
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					// nextIndex: 初始化为 Leader 最新日志 + 1
					rf.nextIndex[i] = lastLogIndex + 1

					if i == rf.me {
						// 【关键修正】Leader 必须知道自己有所有日志
						rf.matchIndex[i] = lastLogIndex
					} else {
						// 【关键修正】Follower 初始化为 0
						// 虽然我们可以乐观地认为是 lastIncludedIndex，但 0 最安全最标准
						rf.matchIndex[i] = 0
					}
				}
				rf.persist()
				rf.heartTimeDeadline = time.Now().Add(100 * time.Millisecond)
				rf.mu.Unlock()
				go rf.runAppendEntries()

				return
			}

		}
		rf.mu.Unlock()
	}

}

// func (rf *Raft) runHeartbeat() {
// 	rf.mu.Lock()
// 	term := rf.currentTerm
// 	LeaderId := rf.me
// 	rf.mu.Unlock()
// 	type heartResult struct {
// 		Reply   *AppendEntriesReply
// 		Success bool
// 	}
// 	// Create a channel to collect heartbeat results
// 	heartch := make(chan heartResult, len(rf.peers)-1)

// 	args := &AppendEntriesArgs{}
// 	args.Term = term
// 	args.LeaderId = LeaderId
// 	for i := 0; i < len(rf.peers); i++ {
// 		if i != rf.me {

// 			// fill in other fields
// 			go func(server int) {
// 				reply := &AppendEntriesReply{}
// 				ok := rf.sendAppendEntries(server, args, reply)
// 				heartch <- heartResult{Reply: reply, Success: ok}
// 			}(i)
// 		}
// 	}

// 	for i := 0; i < len(rf.peers)-1; i++ {
// 		result := <-heartch
// 		rf.mu.Lock()
// 		if result.Success && result.Reply.Term > rf.currentTerm {
// 			rf.state = Follower
// 			rf.currentTerm = result.Reply.Term
// 			rf.votedFor = -1
// 			rf.mu.Unlock()
// 			return
// 		}
// 		rf.mu.Unlock()
// 	}

// }

func (rf *Raft) runAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				if rf.nextIndex[server] <= rf.lastIncludedIndex {
					rf.mu.Unlock()
					rf.sendInstallSnapshot(server)

					return
				}
				args := &AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[server] - 1

				if args.PrevLogIndex == rf.lastIncludedIndex {
					args.PrevLogTerm = rf.lastIncludedTerm
				} else {
					args.PrevLogTerm = rf.logs[args.PrevLogIndex-rf.lastIncludedIndex].Term
				}
				args.LeaderCommit = rf.commitIndex
				if len(rf.logs)-1+rf.lastIncludedIndex >= rf.nextIndex[server] {
					entriesToSend := make([]Log, len(rf.logs[rf.nextIndex[server]-rf.lastIncludedIndex:]))
					copy(entriesToSend, rf.logs[rf.nextIndex[server]-rf.lastIncludedIndex:])
					args.Entries = entriesToSend
				} else {
					args.Entries = nil
				}

				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, args, reply)
				if ok {
					rf.mu.Lock()

					if rf.state != Leader || args.Term != rf.currentTerm {
						rf.mu.Unlock()
						return
					}
					if reply.Term > rf.currentTerm {

						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						newMatchIndex := args.PrevLogIndex + len(args.Entries)
						if newMatchIndex > rf.matchIndex[server] {
							rf.matchIndex[server] = newMatchIndex
							rf.nextIndex[server] = newMatchIndex + 1
							oldCommit := rf.commitIndex
							for N := len(rf.logs) - 1 + rf.lastIncludedIndex; N > rf.commitIndex; N-- {
								count := 1
								for j := 0; j < len(rf.peers); j++ {
									if j != rf.me && rf.matchIndex[j] >= N && rf.logs[N-rf.lastIncludedIndex].Term == rf.currentTerm {
										count += 1
									}
								}
								if count > (len(rf.peers) / 2) {
									rf.commitIndex = N
									break
								}
							}
							if rf.commitIndex > oldCommit {
								rf.applyCond.Signal()
							}
						}
						rf.mu.Unlock()
					} else {
						lastIndexWithTerm := -1
						for i := len(rf.logs) - 1; i >= 0; i-- {
							if rf.logs[i].Term == reply.XTerm {
								lastIndexWithTerm = i
								break
							}
						}
						if reply.XTerm == -1 {
							DPrintf("leader %d recv XTerm=-1 from %d XLen=%d XIndex=%d leaderLastIncl=%d nextIdxBefore=%d", rf.me, server, reply.XLen, reply.XIndex, rf.lastIncludedIndex, rf.nextIndex[server])
							if reply.XLen != -1 {
								rf.nextIndex[server] = reply.XLen
							} else {
								// follower 告诉我们 PrevLogIndex 已经落在它的快照里
								if reply.XIndex-1 <= rf.lastIncludedIndex {
									// 我们也已经截断了这部分日志，只能退回去触发 InstallSnapshot
									rf.nextIndex[server] = reply.XIndex - 1
									if rf.nextIndex[server] < 1 {
										rf.nextIndex[server] = 1
									}
								} else {
									// follower 的快照比我们更新，直接把 nextIndex 提到它的快照之后
									rf.nextIndex[server] = reply.XIndex
								}
							}
						} else if lastIndexWithTerm != -1 {
							rf.nextIndex[server] = lastIndexWithTerm + 1 + rf.lastIncludedIndex
						} else {
							rf.nextIndex[server] = reply.XIndex
						}
						rf.mu.Unlock()
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	DPrintf("leader %d sending snapshot to %d upTo=%d term=%d", rf.me, server, rf.lastIncludedIndex, rf.currentTerm)
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	reply := &InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader || args.Term != rf.currentTerm {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			return
		}
		// follower 至少拥有到 snapshot 的日志，将 matchIndex/nextIndex 同步到该位置
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		oldCommit := rf.commitIndex
		for N := len(rf.logs) - 1 + rf.lastIncludedIndex; N > rf.commitIndex; N-- {
			count := 1
			for j := 0; j < len(rf.peers); j++ {
				if j != rf.me && rf.matchIndex[j] >= N && rf.logs[N-rf.lastIncludedIndex].Term == rf.currentTerm {
					count += 1
				}
			}
			if count > (len(rf.peers) / 2) {
				rf.commitIndex = N
				break
			}
		}
		if rf.commitIndex > oldCommit {
			rf.applyCond.Signal()
		}
	}

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	// reset election timeout on any InstallSnapshot from current leader
	ms := 300 + (rand.Int63() % 50)
	rf.electTimeDeadline = time.Now().Add(time.Duration(ms) * time.Millisecond)
	rf.state = Follower

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	// Save log entries that are after the snapshot
	index := args.LastIncludedIndex
	tempLog := make([]Log, 0)
	tempLog = append(tempLog, Log{Term: args.LastIncludedTerm})

	// Optimize: keep logs that follow the snapshot if they match
	entryIndex := index - rf.lastIncludedIndex
	if entryIndex > 0 && entryIndex < len(rf.logs) {
		if rf.logs[entryIndex].Term == args.LastIncludedTerm {
			tempLog = append(tempLog, rf.logs[entryIndex+1:]...)
		}
	}
	rf.logs = tempLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	//3D
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, args.Data)

	reply.Term = rf.currentTerm

	if args.LastIncludedIndex > rf.lastApplied {
		applymsg := raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}

		// Temporarily unlock to send snapshot to avoid blocking while holding lock
		rf.mu.Unlock()
		rf.applyCh <- applymsg
		rf.mu.Lock()

		if rf.lastApplied < args.LastIncludedIndex {
			rf.lastApplied = args.LastIncludedIndex
		}
	}

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	// 5. 唤醒 applier (防止 commitIndex > lastIncludedIndex 但 applier 还在睡)
	rf.applyCond.Signal()
	rf.mu.Unlock()
}
func (rf *Raft) applier(applyCh chan raftapi.ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()

			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}

		if rf.lastApplied < rf.lastIncludedIndex {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if rf.commitIndex > rf.lastApplied {
			nextIndex := rf.lastApplied + 1 - rf.lastIncludedIndex

			if nextIndex > 0 && nextIndex < len(rf.logs) {
				rf.lastApplied += 1
				applyMsg := raftapi.ApplyMsg{}
				applyMsg.CommandValid = true
				applyMsg.Command = rf.logs[nextIndex].Command
				applyMsg.CommandIndex = rf.lastApplied
				rf.mu.Unlock()
				applyCh <- applyMsg
			} else {
				// Don't modify lastApplied here!
				// If lastApplied < lastIncludedIndex: wait for InstallSnapshot to fix it
				// If it's another boundary case: wait for state to stabilize
				rf.mu.Unlock()
				time.Sleep(5 * time.Millisecond)
			}
		} else {
			rf.mu.Unlock()
		}
	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	rf.currentTerm = 0
	rf.votedFor = -1
	//dummy log at index 0
	rf.logs = make([]Log, 1)
	rf.logs[0] = Log{Command: nil, Term: 0}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.readPersist(persister.ReadRaftState())
	rf.heartTimeDeadline = time.Now().Add(100 * time.Millisecond)
	ms := 300 + (rand.Int63() % 50)
	rf.electTimeDeadline = time.Now().Add(time.Duration(ms) * time.Millisecond)
	rf.state = Follower
	rf.applyCh = applyCh
	// rf.lastIncludedIndex = 0
	// rf.lastIncludedTerm = 0
	if rf.lastIncludedIndex > 0 {
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
	}
	// initialize from state persisted before a crash

	rf.applyCond = sync.NewCond(&rf.mu)

	// start applier goroutine to apply committed log entries to state machine
	go rf.applier(applyCh)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "math/rand"
import "time"

// import "bytes"
// import "encoding/gob"

// ApplyMsg ...
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// LogEntry ...
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// Role enum
type Role byte

const (
	_ Role = iota
	// LEADER ...
	LEADER
	// CANDICATE ...
	CANDICATE
	// FOLLOWER ...
	FOLLOWER

	// HBINTERVAL ...
	// heartbeat interval, 50ms
	HBINTERVAL = 50 * time.Millisecond
)

// Raft ...
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all server
	// latest term server has seen
	currentTerm int
	// candidateId that received vote in current term, -1 for none
	votedFor int
	// log entries
	log []LogEntry

	// volatile state on all server
	// index of highest log entry known to be commited
	commitIndex int
	// index of highest log entry applied to state machine
	lastApplied int

	// volatile state on leader
	// index of the next log entry to send to server, leader only
	nextIndex []int
	// index of the highest log entry known to be replicated to server, leader only
	matchIndex []int

	// Role
	role Role

	// record candidate vote
	voteCount int
	// heartbeat received channel
	chanHeartbeat chan bool
	// received candidate grant vote channel
	chanGrantVote chan bool
	// change to leader channel
	chanLeader chan bool
}

// GetState ...
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.IsLeader()
}

func (rf *Raft) getLastLogIndex() int {
	// is need len check if rf.log always have value?

	l := len(rf.log)
	if 0 != l {
		return rf.log[l-1].Index
	}

	return -1
}

func (rf *Raft) getLastLogTerm() int {
	// is need len check if rf.log always have value?

	l := len(rf.log)
	if 0 != l {
		return rf.log[l-1].Term
	}

	return -1
}

// IsLeader ...
func (rf *Raft) IsLeader() bool {
	return rf.role == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

// RequestVoteArgs ...
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandicatedID int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply ...
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote ...
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLogNewer := func() bool {
		if args.LastLogTerm > rf.getLastLogTerm() {
			return true
		}

		if args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex() {
			return true
		}

		return false
	}

	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		goto RET_FALSE
	}

	if args.Term > rf.currentTerm {
		// new term vote
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandicatedID {
		if !isLogNewer() {
			goto RET_FALSE
		}

		// keep follower role
		rf.chanGrantVote <- true

		rf.votedFor = args.CandicatedID
		rf.role = FOLLOWER
		reply.Term = args.Term
		reply.VoteGranted = true
		return
	}

RET_FALSE:
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.role != CANDICATE {
			return ok
		}

		term := rf.currentTerm
		if args.Term != term {
			// not vote for this term, skip
			// happend when a new leader at candidate
			return ok
		}

		if reply.Term > term {
			// new leader, change to follower
			rf.currentTerm = reply.Term
			rf.role = FOLLOWER
			rf.votedFor = -1
		}

		if reply.VoteGranted {
			rf.voteCount++
			if rf.role == CANDICATE && rf.voteCount > len(rf.peers)/2 {
				// when CANDICATE role and get voteCount by majority
				rf.role = FOLLOWER
				// notify to change to leader
				rf.chanLeader <- true
			}
		}
	}

	return ok
}

func (rf *Raft) broadcastRequestVote() {
	consArgs := func() *RequestVoteArgs {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		return &RequestVoteArgs{Term: rf.currentTerm,
			CandicatedID: rf.me,
			LastLogTerm:  rf.getLastLogTerm(),
			LastLogIndex: rf.getLastLogIndex(),
		}
	}

	args := consArgs()

	for i := range rf.peers {
		if i != rf.me && rf.role == CANDICATE {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)
			}(i)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.role != LEADER {
			// while send but leader change
			return ok
		}

		if args.Term != rf.currentTerm {
			// new term
			return ok
		}

		if reply.Term > rf.currentTerm {
			// follower has new term and this leader
			rf.currentTerm = reply.Term
			rf.role = FOLLOWER
			rf.votedFor = -1
			return ok
		}

		if reply.Success {
		}
	}

	return ok
}

func (rf *Raft) broadcastHeartbeat() {
	consArgs := func() *AppendEntriesArgs {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		return &AppendEntriesArgs{Term: rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: rf.getLastLogIndex(),
			PrevLogTerm:  rf.getLastLogTerm(),
			LeaderCommit: rf.commitIndex,
		}
	}

	args := consArgs()

	for i := range rf.peers {
		if i != rf.me && rf.role == LEADER {
			go func(i int) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(i, args, &reply)
			}(i)
		}
	}
}

// AppendEntriesArgs ...
type AppendEntriesArgs struct {
	// leader's term
	Term int
	// for follower redirect to leader
	LeaderID int
	// index of log entry immediately preceding
	PrevLogIndex int
	// term of PrevLogIndex entry
	PrevLogTerm int
	// log entries to store, empty for heartheat
	Entries []LogEntry
	// leader's commitIndex
	LeaderCommit int
}

// AppendEntriesReply ...
type AppendEntriesReply struct {
	// currentTerm, for leader to update itself
	Term int
	// true if follower contained entry matching prevLogIndex and prevLogIndex
	Success bool
}

// AppendEntries ...
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		goto RET_FALSE
	}

	rf.chanHeartbeat <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
	}

	reply.Term = args.Term

	if args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		goto RET_FALSE
	}

	if 0 == len(args.Entries) {
		// heartbeat, reset timeout
	}

RET_FALSE:
	reply.Term = rf.currentTerm
	reply.Success = false
	return
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// Make ...
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{})

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.role = FOLLOWER

	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// leader election backgroud goroutine
	go func() {
		for {
			switch rf.role {
			case FOLLOWER:
				select {
				case <-rf.chanHeartbeat:
					// do nothing
				case <-rf.chanGrantVote:
					// do nothing
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
					// received heartbeat or vote timeout, change to candidate
					rf.role = CANDICATE
				}
			case LEADER:
				rf.broadcastHeartbeat()
				time.Sleep(HBINTERVAL)
			case CANDICATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.mu.Unlock()

				go rf.broadcastRequestVote()

				select {
				case <-time.After(time.Duration(time.Duration(rand.Int63()%333+500)) * time.Millisecond):
					// timeout, do nothing, and start next candicate
				case <-rf.chanHeartbeat:
					rf.role = FOLLOWER
				case <-rf.chanLeader:
					rf.mu.Lock()
					rf.role = LEADER
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}
			}
		}
	}()

	return rf
}

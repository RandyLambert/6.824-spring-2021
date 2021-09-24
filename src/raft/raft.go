package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	FollowerStatus  = 1
	CandidateStatus = 2
	LeaderStatus	= 3
)

const (
	ElectionTimeout  = time.Millisecond * 300
	HeartBeatTimeout = time.Millisecond * 100
	ApplyInterval    = time.Millisecond * 100 // apply log
	RPCTimeout       = time.Millisecond * 100
	MaxLockTime      = time.Millisecond * 10 // debug
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status           int32              // raft状态
	currentTerm      int				 // 服务器已知最新的任期（在服务器首次启动的时候初始化为0，单调递增）
	votedFor         int				 // 当前任期内收到选票的候选者id 如果没有投给任何候选者 则为空
	logEntries        []LogEntry		 // 日志条目;每个条目包含了用于状态机的命令，以及领导者接收到该条目时的任期（第一个索引为1）
	applyCh           chan ApplyMsg
	commitIndex       int       // 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied       int       // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	nextIndex         []int     // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1）
	matchIndex        []int     // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
	leaderId          int       // 领导者ID 因此跟随者可以对客户端进行重定向（译者注：跟随者根据领导者id把客户端的请求重定向到领导者，比如有时客户端把请求发给了跟随者而不是领导者）
	heartBeat         chan bool // 心跳,打断超时时间

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("GetState, false")
	return rf.currentTerm, rf.status == LeaderStatus
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
}

// 由领导者调用 用于日志条目的复制 同时也被当做心跳使用
type AppendEntriesArgs struct {
	Term         int        // 领导者的任期
	LeaderId     int        // 领导者ID 因此跟随者可以对客户端进行重定向（译者注：跟随者根据领导者id把客户端的请求重定向到领导者，比如有时客户端把请求发给了跟随者而不是领导者）
	PrevLogIndex int        // 紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int        // 紧邻新日志条目之前的那个日志条目的任期
	LogEntries   []LogEntry // 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	LeaderCommit int        // 领导者的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  // 当前任期，对于领导者而言
	Success bool // 结果为真 如果跟随者所含有的条目和prevLogIndex以及prevLogTerm匹配上了
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	//rf.mu.Lock()
	//if args.Term > rf.currentTerm {
	//	rf.currentTerm = args.Term
	//	rf.status = FollowerStatus
	//	rf.votedFor = -1
	//}
	//
	//if args.Term == rf.currentTerm && args.PrevLogIndex <= len(rf.logEntries) {
	////&& (args.PrevLogIndex == 0 || rf.logEntries[args.PrevLogIndex-1].Term == args.PrevLogTerm) {
	//	rf.status = FollowerStatus
	//	reply.Success = true
	//	//DPrintf("AppendEntries")
	//}
	////else {
	////	reply.Success = false
	////}
	//reply.Term = rf.currentTerm
	//rf.heartBeat <- true
	//rf.mu.Unlock()

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = FollowerStatus
		rf.heartBeat<-true


	} else if args.Term == rf.currentTerm {
		//if rf.status != LeaderStatus {
		//rf.status = FollowerStatus
		//}
		rf.heartBeat<-true
	}
	// todo 2B
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期号
	CandidateId  int // 请求选票的候选人的 id
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前任期号
	VoteGranted bool //
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isVote := false
	if args.Term < rf.currentTerm {
		isVote = false
	} else if args.Term == rf.currentTerm {
		//laseLogIndex, lastLogTerm := rf.getLastLogIndexAndTermNotLock()
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			isVote = true
		//} else if rf.votedFor == -1 && laseLogIndex <= args.LastLogIndex && lastLogTerm <= args.LastLogTerm {

		} else {
			isVote = false
		}

	} else if args.Term > rf.currentTerm {
		isVote = true
	}

	if isVote == false {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else {
		rf.currentTerm = args.Term
		rf.status = FollowerStatus
		rf.votedFor = args.CandidateId
		//DPrintf("rd.me %d before %d become Follower",rf.me,rf.status)

		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.heartBeat <- true
	}
	//rf.mu.Lock()
	//if args.Term > rf.currentTerm {
	//	rf.currentTerm = args.Term
	//	rf.status = FollowerStatus
	//	rf.votedFor = args.CandidateId
	//}
	//if args.Term == rf.currentTerm {
	//	if rf.votedFor == args.CandidateId || rf.votedFor == -1{
	//		rf.votedFor = args.CandidateId
	//		reply.VoteGranted = true
	//		rf.heartBeat <- true
	//	}
	//}
	//reply.Term = rf.currentTerm
	//rf.mu.Unlock()
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
	return ok
}


//
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
// Term. the third return value is true if this server believes it is
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
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getLastLogIndexAndTermNotLock() (int, int){
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rf.logEntries) != 0 {
		lastLogIndex = len(rf.logEntries)
		lastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
	}
	return lastLogIndex, lastLogTerm
}


// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) initiateElection() {

	rf.mu.Lock()
	if rf.status == FollowerStatus {
		rf.status = CandidateStatus
	}

	if rf.status != CandidateStatus {
		rf.mu.Unlock()
		return
	}
	// 当前的任期值加1
	rf.currentTerm++
	term := rf.currentTerm
	me := rf.me
	// 有问题
	lastLogIndex,lastLogTerm := rf.getLastLogIndexAndTermNotLock()
	// 给自己投票
	rf.votedFor = me
	count := 1
	rf.mu.Unlock()

	go func() {
		var muCount sync.Mutex
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(index int) {
					args := &RequestVoteArgs{
						Term:         term,
						CandidateId:  me,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
					}
					replay := &RequestVoteReply{}
					flag := rf.sendRequestVote(index, args, replay)
					muCount.Lock()
					//DPrintf("replay.Term = %d, term = %d, replay.VoteGranted = %t",replay.Term,term,replay.VoteGranted)
					if flag == true && replay.Term == term && replay.VoteGranted {
						count++
					}
					rf.mu.Lock()
					if count == len(rf.peers)/2+1 && rf.currentTerm == term && rf.status == CandidateStatus {
						// 选举成功
						rf.heartBeat<-true
						rf.status = LeaderStatus
						DPrintf("rf.me %d become leader", rf.me)
						rf.sendHeartBeat()
					}
					rf.mu.Unlock()
					muCount.Unlock()
				}(i)
			}
		}
	}()

}

func (rf *Raft) sendHeartBeat() {
	go func(){
		rf.mu.Lock()
		term := rf.currentTerm
		leaderId := rf.me
		leaderCommit := rf.commitIndex
		rf.mu.Unlock()

		for {
			currentTerm, isLeader := rf.GetState()
			if currentTerm != term || !isLeader {
				break
			}
			for i := 0; i < len(rf.peers); i++ {
				go func(index int) {
					// todo 有问题
					args := &AppendEntriesArgs{
						Term:         term,
						LeaderId:     leaderId,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						LogEntries:   nil,
						LeaderCommit: leaderCommit,
					}

					replay := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(index, args, replay)
					rf.mu.Lock()
					if ok && replay.Term > rf.currentTerm {
						DPrintf("rf.status = %d, rf.currentTerm = %d, replay.Term = %d",rf.status,rf.currentTerm,replay.Term)
						rf.currentTerm = replay.Term
						rf.votedFor = -1
						rf.status = FollowerStatus
					}
					rf.mu.Unlock()
				}(i)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
}
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep()
		sleepTime,_ := rand.Int(rand.Reader, big.NewInt(150))
		timeOut := time.After(time.Duration(sleepTime.Int64() + 150) * time.Millisecond)
		//DPrintf("%d",sleepTime)
		select {
		case <-timeOut:
			rf.initiateElection()
		case <-rf.heartBeat:
			break
			//DPrintf("heartBeat")
		}
	}
}

//
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
	rf.applyCh = applyCh
	rf.status = FollowerStatus
	rf.votedFor = -1
	rf.heartBeat = make(chan bool, 1)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

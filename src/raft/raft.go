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
	_ "net/http/pprof"
	"os"
	"runtime"
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
	FollowerState  = 1
	CandidateState = 2
	LeaderState    = 3
)

const (
	ElectionTimeoutMin  = 150
	ElectionTimeoutMax  = 300
	HeartBeatTimeout = time.Millisecond * 100
	RPCTimeout       = time.Millisecond * 100
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
	state       int32      // raft状态
	currentTerm int        // 服务器已知最新的任期（在服务器首次启动的时候初始化为0，单调递增）
	votedFor    int        // 当前任期内收到选票的候选者id 如果没有投给任何候选者 则为空
	logEntries  []LogEntry // 日志条目;每个条目包含了用于状态机的命令，以及领导者接收到该条目时的任期（第一个索引为1）
	applyCh     chan ApplyMsg
	commitIndex int       // 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int       // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	nextIndex   []int     // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1）
	matchIndex  []int     // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
	leaderId    int       // 领导者ID 因此跟随者可以对客户端进行重定向（译者注：跟随者根据领导者id把客户端的请求重定向到领导者，比如有时客户端把请求发给了跟随者而不是领导者）
	heartBeat   chan bool // 心跳,打断超时时间
	applyCond   *sync.Cond // 唤醒 apply 干活

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LeaderState
}

func (rf *Raft) changeStateNotLock(state int32) {
	rf.state = state
	if state == CandidateState {
		rf.votedFor = rf.me
	} else if state == LeaderState {
		rf.currentTerm++
		rf.leaderId = rf.me

		lastLogIndex, _ := rf.getLastLogIndexAndTermNotLock()
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0;i < len(rf.peers);i++ {
			rf.nextIndex[i] = lastLogIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = lastLogIndex
	}
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
	Conflict bool

	XTerm	int // Follower 这个是Follower中与Leader冲突的Log对应的任期号
	// 在之前（7.1）有介绍Leader会在prevLogTerm中带上本地Log记录中，前一条Log的任期号。
	// 如果Follower在对应位置的任期号不匹配，它会拒绝Leader的AppendEntries消息，并将自己的任期号放在XTerm中。
	// 如果Follower在对应位置没有Log，那么这里会返回 -1。
	XIndex	int // 这个是Follower中，对应任期号为XTerm的第一条Log条目的槽位号。
	XLen	int // 如果Follower在对应位置没有Log，那么XTerm会返回-1，XLen表示空白的Log槽位数。
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = args.LeaderId
		rf.changeStateNotLock(FollowerState)
		rf.heartBeat<-true
		return
	}

	if args.Term < rf.currentTerm {
		return
	}

	if rf.state == CandidateState {
		rf.changeStateNotLock(FollowerState)
	}

	if args.Term == rf.currentTerm {
		rf.heartBeat<-true
		// TODO 不一样
		// 有 log
		if len(args.LogEntries) != 0 {
			// log 匹配成功
			lastLogIndex, _ := rf.getLastLogIndexAndTermNotLock()
			if args.PrevLogIndex > lastLogIndex { // log 匹配失败
				reply.XLen = len(rf.logEntries)
				reply.XTerm = -1
				reply.XIndex = -1
				reply.Success = false

				reply.Conflict = true
			} else if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
				xTerm := rf.logEntries[args.PrevLogIndex].Term
				for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
					if rf.logEntries[xIndex - 1].Term != xTerm {
						reply.XIndex = xIndex
						break
					}
				}
				reply.XTerm = xTerm
				reply.XLen = len(rf.logEntries)
				reply.Success = false

				reply.Conflict = true
			} else {
				for index, entry := range args.LogEntries {
					// append entries rpc 3
					entryIndex := args.PrevLogIndex + index + 1
					lastLogIndex, _ = rf.getLastLogIndexAndTermNotLock()
					if entryIndex <= lastLogIndex && rf.logEntries[entryIndex].Term != entry.Term {
						// go 切片截断
						DPrintf("ttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt")
						//entries := make([]LogEntry, entryIndex )
						//copy(entries, args.Entries)
						rf.logEntries = rf.logEntries[0:entryIndex+1]
						//rf.persist()
					}

					// append entries rpc 4
					if entryIndex > lastLogIndex {
						rf.logEntries = append(rf.logEntries, args.LogEntries[index:]...)
						DPrintf("in log append lastLogIndex = %d",lastLogIndex)
						//rf.persist()
						break
					}
				}

				// append entries rpc 5
				if args.LeaderCommit > rf.commitIndex {
					if args.LeaderCommit < lastLogIndex {
						rf.commitIndex = args.LeaderCommit
					} else {
						rf.commitIndex = lastLogIndex
					}
					DPrintf("rf.applyCond.Broadcast() rf.me = %d",rf.me)
					rf.applyCond.Broadcast()
				}
				reply.Success = true
			}
		} else {
			if args.LeaderCommit > rf.commitIndex {
				lastLogIndex, _ := rf.getLastLogIndexAndTermNotLock()
				if args.LeaderCommit < lastLogIndex {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = lastLogIndex
				}
				DPrintf("rf.applyCond.Broadcast() rf.me = %d",rf.me)
				rf.applyCond.Broadcast()
			}
			reply.Success = true
		}
	}
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
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTermNotLock()
			if lastLogTerm < args.LastLogTerm ||
				(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
				isVote = true
			} else {
				isVote = false
			}
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
		rf.changeStateNotLock(FollowerState)
		rf.votedFor = args.CandidateId
		//DPrintf("rd.me %d before %d become Follower",rf.me,rf.state)

		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.heartBeat <- true
	}

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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// index 是当前最大值
	index,_ := rf.getLastLogIndexAndTermNotLock()
	// 下一个index的大小
	index++
	term := rf.currentTerm
	isLeader := rf.state == LeaderState
	if isLeader {
		rf.logEntries = append(rf.logEntries , struct {
			Term    int
			Command interface{}
		}{Term: term, Command: command})
		rf.matchIndex[rf.me] = index
		DPrintf("Start|rf.me = %d is Leader, lastLogIndex = %d",rf.me,index)
	}
	//rf.heartBeat <- true
	DPrintf("index = %d, term = %d, isLeader = %t, rf.me = %d",index, term, isLeader, rf.me)
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
	lastLogIndex := len(rf.logEntries) - 1
	lastLogTerm := rf.logEntries[lastLogIndex].Term

	return lastLogIndex, lastLogTerm
}


// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) initiateElection() {

	rf.mu.Lock()
	if rf.state == LeaderState {
		rf.mu.Unlock()
		return
	}
	// 当前的任期值加1,并改变状态为Candidate
	rf.changeStateNotLock(CandidateState)
	me := rf.me
	term := rf.currentTerm + 1
	lastLogIndex,lastLogTerm := rf.getLastLogIndexAndTermNotLock()
	// 给自己投票,记票为1
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
					if flag == true && replay.Term == term && replay.VoteGranted {
						count++
					}
					rf.mu.Lock()
					if count == len(rf.peers)/2+1 && rf.currentTerm+1 == term && rf.state == CandidateState {
						// 选举成功
						rf.heartBeat<-true
						rf.changeStateNotLock(LeaderState)
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

//var a int
//var mmu sync.Mutex
func (rf *Raft) sendHeartBeat() {
	go func(){
		rf.mu.Lock()
		term := rf.currentTerm
		leaderId := rf.me
		rf.mu.Unlock()

		for !rf.killed() {
			for i := 0; i < len(rf.peers); i++ {
				currentTerm, isLeader := rf.GetState()
				if currentTerm != term || !isLeader {
					return
				}
				if i != leaderId {
					go func(index int) {
						rf.mu.Lock()
						if rf.state != LeaderState {
							rf.mu.Unlock()
							return
						}
						LastLogIndex, _ := rf.getLastLogIndexAndTermNotLock()
						leaderCommit := rf.commitIndex
						PrevLogIndex := -1
						PrevLogTerm := -1
						var LogEntries []LogEntry
						if LastLogIndex+1 > rf.nextIndex[index] {
							PrevLogIndex = rf.nextIndex[index] - 1
							PrevLogTerm = rf.logEntries[PrevLogIndex].Term
							LogEntries = make([]LogEntry, LastLogIndex - rf.nextIndex[index] + 1)
							copy(LogEntries,rf.logEntries[rf.nextIndex[index]:LastLogIndex+1])
							DPrintf("PrevLogIndex = %d, raftId = %d, rf.nextIndex[index] = %d, leaderId = %d, rf.logEntries[PrevLogIndex].Term = %d, rf.currentTerm = %d, len(LogEntries) = %d\n",PrevLogIndex,index,rf.nextIndex[index], leaderId, rf.logEntries[PrevLogIndex].Term,rf.currentTerm,len(LogEntries))
						}
						rf.mu.Unlock()

						args := &AppendEntriesArgs{
							Term:         term,
							LeaderId:     leaderId,
							PrevLogIndex: PrevLogIndex,
							PrevLogTerm:  PrevLogTerm,
							LogEntries:   LogEntries,
							LeaderCommit: leaderCommit,
						}

						replay := &AppendEntriesReply{}
						ok := rf.sendAppendEntries(index, args, replay)
						if !ok {
							return
						}
						rf.mu.Lock()
						if rf.state != LeaderState {
							rf.mu.Unlock()
							return
						}
						if ok && replay.Term > rf.currentTerm {
							DPrintf("rf.state = %d, rf.currentTerm = %d, replay.Term = %d",rf.state,rf.currentTerm,replay.Term)
							rf.currentTerm = replay.Term
							rf.votedFor = -1
							rf.changeStateNotLock(FollowerState)
						}

						if ok && replay.Term == rf.currentTerm {
							// 成功或者失败需要有不同的操作
							// TODO debug this
							//DPrintf("replay.XTerm = %d ,replay.XIndex = %d ,replay.XLen = %d, replay.Success = %t, replay.Conflict = %t",replay.XTerm, replay.XIndex, replay.XLen, replay.Success ,replay.Conflict)
							if len(args.LogEntries) != 0 {
								if replay.Success == true {
									// 单纯的心跳不带log
									match := args.PrevLogIndex + len(args.LogEntries)
									if rf.matchIndex[index] < match {
										rf.matchIndex[index] = match
									}
									next := match + 1
									if rf.nextIndex[index] < next {
										rf.nextIndex[index] = next
									}
									DPrintf("rf.nextIndex[index] = %d", rf.nextIndex[index])
								} else if replay.Conflict {
									//if rf.nextIndex[index] > 1{
									//	rf.nextIndex[index]--
									//}
									if replay.XTerm == -1 {
										rf.nextIndex[index] = replay.XLen
									} else {
										// 可二分优化
										length := LastLogIndex
										for length >= 0 && rf.logEntries[length].Term != replay.XTerm {
											length--
										}
										if length == -1 {
											rf.nextIndex[index] = replay.XIndex
										} else {
											// TODO 调查length 到底用那个值
											// conflict nextIndex--
											DPrintf("lennnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
											rf.nextIndex[index] = length
										}
									}
								} else if rf.nextIndex[index] > 1 {
									rf.nextIndex[index]--
									DPrintf("because rf.nextIndex[index] > 1 so rf.nextIndex[index] = %d", rf.nextIndex[index])
								}
							}
						}

						//LastLogIndex, _ = rf.getLastLogIndexAndTermNotLock()
						//commitIndexOld := rf.commitIndex
						//for commitIndex := rf.commitIndex + 1;commitIndex <= LastLogIndex; commitIndex++ {
						//	replayNum := 0
						//	for peerIndex := 0;peerIndex < len(rf.peers);peerIndex++ {
						//		if rf.matchIndex[peerIndex] >= commitIndex {
						//			replayNum++
						//		}
						//		if replayNum == len(rf.peers)/2+1 {
						//			rf.commitIndex++
						//			break
						//		}
						//	}
						//	if replayNum < len(rf.peers)/2+1 {
						//		break
						//	}
						//}
						//if rf.commitIndex > commitIndexOld {
						//	DPrintf("commitIndex = %d",rf.commitIndex)
						//	rf.applyCond.Broadcast()
						//}

						// TODO need fix
						LastLogIndex, _ = rf.getLastLogIndexAndTermNotLock()
						for n := rf.commitIndex + 1; n <= LastLogIndex; n++ {
							if rf.logEntries[n].Term != rf.currentTerm {
								continue
							}
							counter := 1
							for serverId := 0; serverId < len(rf.peers); serverId++ {
								if serverId != rf.me && rf.matchIndex[serverId] >= n {
									counter++
								}
								if counter > len(rf.peers)/2 {
									rf.commitIndex = n
									rf.applyCond.Broadcast()
									break
								}
							}
						}
						rf.mu.Unlock()
					}(i)
				}
			}
			time.Sleep(HeartBeatTimeout)
		}
	}()
}
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep()
		sleepTime,_ := rand.Int(rand.Reader, big.NewInt(ElectionTimeoutMin))
		timeOut := time.After(time.Duration(sleepTime.Int64() + ElectionTimeoutMax - ElectionTimeoutMin) * time.Millisecond)
		select {
		case <-timeOut:
			rf.initiateElection()
		case <-rf.heartBeat:
			break
		}
	}
}

func (rf *Raft) applyTicker() {
	for rf.killed() == false {
		DPrintf("in applyTicker rf.commitIndex = %d, rf.lastApplied = %d, rf.me = %d",rf.commitIndex,rf.lastApplied, rf.me)
		rf.mu.Lock()
		lastLogIndex,_ := rf.getLastLogIndexAndTermNotLock()
		if rf.commitIndex > rf.lastApplied && lastLogIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logEntries[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
		rf.mu.Unlock()
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
	//go func() {
	//	log.Println(http.ListenAndServe("localhost:6060", nil))
	//}()
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.state = FollowerState
	rf.votedFor = -1
	rf.heartBeat = make(chan bool, 1)
	rf.logEntries = append(rf.logEntries, struct {
		Term    int
		Command interface{}
	}{Term: 1, Command: nil})
	rf.applyCond = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyTicker()
	go func() {
		for !rf.killed() {
			time.Sleep(1 * time.Second)
			//DPrintf("%d", runtime.NumGoroutine())
			if runtime.NumGoroutine() > 2000 {
				rf.Kill()
				os.Exit(0)
			}
		}
	}()

	return rf
}

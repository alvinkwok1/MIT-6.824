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

import (
	"encoding/json"
	"log"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// raft state
const (
	FOLLOWER  int = 1
	CANDIDATE int = 2
	LEADER    int = 3
)

const (
	MaxElectionTime = 500
	MinElectionTime = 200
	HeartbeatTime   = 100
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// 对log的抽象，会包含一个log的任期和指令
// 要进行传输，因此暴露出属性
type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// 该属性用于向状态机提交数据
	applyCh chan ApplyMsg
	// state a Raft server must maintain.
	state int // follower,candidate,leader
	// 以下为raft中的持久化状态
	currentTerm int        // 当前任期，初始化为0,选举时自增
	voteFor     int        // 投票记录，每个任期只会投给一个候选人
	log         []LogEntry // 日志
	// 以下状态用来统计投票数量
	voteCount int
	// 以下状态为所有服务器共用
	commitIndex int // 已知的已提交的最大log的索引，初始化为0，自增
	lastApplied int // 已应用到状态机的最大索引，初始化为0，自增
	// 以下状态是leader所维护的状态
	nextIndex  []int // 对于每一个follower，初始化一个下一个提交索引，初始化的值为leader节点的日志索引 + 1
	matchIndex []int // 对于每一个follower，用来存储leader与follower已匹配的索引位置，用来计算leader上的哪些日志可以提交

	// 以下属性用来做计时
	electionTimeout  int       // 选举超时时间
	nextElectionTime time.Time // 下次选举时间

	// 以下属性用来控制goroutine
	runCopyLogTask   int32 // 是否继续运行copy日志的任务
	runHeartbeatTask int32 // 是否继续运行心跳任务
}

type AppendEntriesArgs struct {
	Term         int // leader的任期
	LeaderId     int // leader 的id
	PrevLogTerm  int // 紧邻新日志条目之前的那个日志条目的任期
	PrevLogIndex int // 紧邻新日志条目之前的那个日志条目的索引
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	// Your code here (2A).
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期
	CandidateId  int // 候选人ID
	LastLogIndex int // 候选人的最新索引
	LastLogTerm  int // 候选人的最新日志对应的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 被请求节点的任期
	VoteGranted bool // 是否投票给了自己
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	b, _ := json.Marshal(*args)
	log.Printf("节点%d接收到投票请求[%s]\n", rf.me, b)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 任期小的情况就要直接转换为FOLLOWER并清空投票信息
	if rf.currentTerm < args.Term {
		rf.changeState(FOLLOWER)
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}

	// 检查任期和当前节点的投票情况
	if rf.currentTerm > args.Term || (rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// 日志匹配检查
	if rf.checkLogIsLatest(args.LastLogIndex, args.LastLogTerm) {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		// 投票后要更新自己的下一次选举时间
		rf.updateNextElectionTime()
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

// 检查日志是不是最新的
func (rf *Raft) checkLogIsLatest(lastLogIndex int, lastLogTerm int) bool {
	// 先检查任期是否一致
	currentLastLogIndex := len(rf.log) - 1
	currentLastLog := rf.log[currentLastLogIndex]
	if lastLogTerm > currentLastLog.Term {
		// leader的任期号大于当前节点的任期号说明leader的日志更新
		return true
	} else if lastLogTerm == currentLastLog.Term {
		// 索引值一样就检查谁的日志更长
		if lastLogIndex >= currentLastLogIndex {
			return true
		}
	}
	return false
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
	// 初始化参数
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	lastLogIndex := len(rf.log) - 1
	args.LastLogIndex = lastLogIndex
	args.LastLogTerm = rf.log[lastLogIndex].Term
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		// 打印响应结果
		b, _ := json.Marshal(reply)
		log.Printf("节点%d接收到节点%d投票响应[%s]\n", rf.me, server, b)
	}
	// 处理响应结果
	rf.mu.Lock()
	if reply.VoteGranted {
		rf.voteCount++
		// 如果投票数量大于集群的一半，则状态变化为leader
		if rf.voteCount > len(rf.peers)/2 {
			rf.changeState(LEADER)
		}
	} else {
		// 响应结果如果任期号比自己大就要立即转换为FOLLOWER
		if reply.Term > rf.currentTerm {
			rf.changeState(FOLLOWER)
			rf.currentTerm = reply.Term
		}
	}
	rf.mu.Unlock()
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 检查当前节点的状态，如果不是LEADER的情况不添加
	isLeader = rf.state == LEADER
	if !isLeader {
		return index, term, isLeader
	} else {
		// 将日志添加到本地
		newLog := LogEntry{}
		newLog.Term = rf.currentTerm
		newLog.Command = command
		rf.log = append(rf.log, newLog)
		// 当前节点的nextIndex 自增
		rf.nextIndex[rf.me] = len(rf.log)
		rf.matchIndex[rf.me] = len(rf.log) - 1
		index = len(rf.log) - 1
		term = rf.currentTerm
		// 添加日志到其他节点
		log.Printf("LEADER %d接收倒了新日志[%v]，分配索引%d和任期%d", rf.me, command, index, term)
	}
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

func (rf *Raft) setTaskFlag(p *int32, v int32) {
	atomic.StoreInt32(p, v)
}

func (rf *Raft) isRunningTask(p *int32) bool {
	z := atomic.LoadInt32(p)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// 每次睡眠100ms
		time.Sleep(time.Duration(100) * time.Millisecond)
		// 睡眠结束后查看当前节点的状态，如果当前节点是LEADER节点则继续睡眠
		rf.tickerHandler()
	}
}

// 是否需要继续维护选举计时器
func (rf *Raft) tickerHandler() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		return
	} else if rf.state == FOLLOWER || rf.state == CANDIDATE {
		// 超时则需要重新选举
		if time.Now().After(rf.nextElectionTime) {
			rf.changeState(CANDIDATE)
		}
	}
	return
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0 // 任期初始化为0
	rf.voteFor = -1
	rf.log = make([]LogEntry, 1) // 初始化log，索引从1开始，索引为0表示空
	rf.log[0].Term = 0

	// 初始化已提交记录索引和已应用索引位置为0
	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.changeState(FOLLOWER)    // raft 初始化状态都是follower
	rf.resetElectionTimeout()   // 初始化选举超时时间间隔
	rf.updateNextElectionTime() // 更新下次选举时间
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) changeState(state int) {
	switch state {
	case FOLLOWER:
		log.Printf("node %v change to FOLLOWER\n", rf.me)
		rf.state = FOLLOWER
		// 清空投票信息
		rf.voteFor = -1 // -1 表示没有给任何人投票
		rf.voteCount = 0
		// 关闭leader节点的心跳任务和日志复制任务
		rf.setTaskFlag(&rf.runHeartbeatTask, 0)
		rf.setTaskFlag(&rf.runCopyLogTask, 0)
		break
	case CANDIDATE:
		log.Printf("node %v change to CADIDATE\n", rf.me)
		rf.state = CANDIDATE
		// 自增任期号
		rf.currentTerm = rf.currentTerm + 1
		// 给自己投票
		rf.voteFor = rf.me
		rf.voteCount = 1
		// 重置选举超时计时器
		rf.resetElectionTimeout()
		rf.updateNextElectionTime()
		// 发送投票信息的RPC给其他服务器
		go rf.election()
		break
	case LEADER:
		// 已经是LEADER的情况就不需要再次触发了
		if rf.state == LEADER {
			return
		}
		log.Printf("node %v change to LEADER\n", rf.me)
		rf.state = LEADER
		// 初始化nextIndex 和matchIndex
		total := len(rf.peers)
		rf.nextIndex = make([]int, total)
		lastLogIndex := len(rf.log) - 1
		// nextIndex初始化为leader的日志索引 + 1
		for i := 0; i < total; i++ {
			rf.nextIndex[i] = lastLogIndex + 1
		}
		// matchIndex 初始化为0
		rf.matchIndex = make([]int, total)
		for i := 0; i < total; i++ {
			rf.matchIndex[i] = 0
		}
		rf.setTaskFlag(&rf.runHeartbeatTask, 1)
		rf.setTaskFlag(&rf.runCopyLogTask, 1)
		// 开启心跳和日志复制任务
		go rf.heartbeats()
		go rf.copyLog()
		break
	default:
		log.Fatalf("未知的状态[%d]", state)
	}
}

// 重置选举时间间隔
func (rf *Raft) resetElectionTimeout() {
	// 初始化选举时间间隔
	rf.electionTimeout = rand.Intn(MaxElectionTime-MinElectionTime) + MaxElectionTime
}

// 更新下一次选举时间
func (rf *Raft) updateNextElectionTime() {
	rf.nextElectionTime = time.Now().Add(time.Duration(rf.electionTimeout) * time.Millisecond)
}

// 开始选举
func (rf *Raft) election() {
	total := len(rf.peers)
	for i := 0; i < total; i++ {
		if i != rf.me {
			serverId := i
			go func() {
				// 发送请求投票RPC
				args := &RequestVoteArgs{}
				reply := &RequestVoteReply{}
				rf.sendRequestVote(serverId, args, reply)
			}()
		}
	}
}

// 当成为LEADER后需要定期向各个FOLLOWER发送心跳信息
func (rf *Raft) heartbeats() {
	for {
		if rf.killed() == false && rf.isRunningTask(&rf.runHeartbeatTask) {
			rf.mu.Lock()
			isLeader := rf.state == LEADER
			rf.mu.Unlock()
			// 不是LEADER的情况结束心跳循环
			if !isLeader {
				return
			}
			// 并行发送追加日志信息
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					serverId := i
					go rf.sendHeartbeat(serverId)
				}
			}
			time.Sleep(HeartbeatTime * time.Millisecond)
		}
	}
}

func (rf *Raft) sendHeartbeat(serverId int) {
	// 检查当前是不是leader，不是leader的情况直接返回，不再发送
	rf.mu.Lock()
	continueSend := rf.state == LEADER
	rf.mu.Unlock()
	if !continueSend {
		return
	}
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}
	// 心跳不复制日志，不关心lastLodIndex
	ok, _ := rf.sendAppendEntries(serverId, true, args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Success == false {
			// 检查响应的任期是否比当前的大
			if rf.currentTerm < reply.Term {
				// 状态回到FOLLOWER
				rf.changeState(FOLLOWER)
				rf.currentTerm = reply.Term
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(server int, isHeartbeat bool, args *AppendEntriesArgs, reply *AppendEntriesReply) (bool, int) {
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	lastLogIndex := len(rf.log) - 1
	nextIndex := rf.nextIndex[server]
	// 检查是否是心跳请求
	if isHeartbeat {
		// 心跳的情况下不处理日志
	} else {
		// 尝试复制最新的日志到FOLLOWER
		if lastLogIndex >= nextIndex {
			args.Entries = rf.log[nextIndex:]
		}
	}
	// 不管是心跳还是正常的AE请求都应该发送preLogIndex和preLogTerm
	// 两者应该是根据nextIndex来计算
	preLogIndex := nextIndex - 1
	args.PrevLogIndex = preLogIndex
	args.PrevLogTerm = rf.log[preLogIndex].Term
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		b, _ := json.Marshal(reply)
		log.Printf("节点%d接收到节点%d的AE响应%s", rf.me, server, b)
	}
	return ok, lastLogIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	b, _ := json.Marshal(args)
	log.Printf("节点%d,接收到AE请求[%s]", rf.me, b)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// 检查任期
	if args.Term > rf.currentTerm {
		rf.changeState(FOLLOWER)
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.updateNextElectionTime()
	lastLogIndex := len(rf.log) - 1

	// 日志不为空的时候不匹配日志
	if lastLogIndex >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		// 将preLogIndex之后的日志删除，并采用leader的日志覆盖
		rf.log = rf.log[:args.PrevLogIndex+1]
		for i := 0; i < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i])
		}
	} else {
		// 如果接收者日志中没有包含这样一个条目 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上,则返回假
		reply.Success = false
		return
	}
	// 根据leader发来的commitIndex 对本地的log进行提交
	if args.LeaderCommit > rf.commitIndex {
		lastLogIndex = len(rf.log) - 1
		rf.commitIndex = min(args.LeaderCommit, lastLogIndex)
	}
	rf.applyLog()
	// 响应成功
	reply.Success = true
	return
}

func min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}
func (rf *Raft) applyLog() {
	// 需要尝试应用日志的状态机
	if rf.commitIndex > rf.lastApplied {
		b, _ := json.Marshal(rf.log)
		log.Printf("节点%d的日志[%s],commitIndex=%d,lastApplied=%d\n", rf.me, b, rf.commitIndex, rf.lastApplied)
		// 从上次应用之后的日志开始应用
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{}
			applyMsg.Command = rf.log[i].Command
			applyMsg.CommandIndex = i
			applyMsg.CommandValid = true
			rf.lastApplied = i
			rf.applyCh <- applyMsg
			log.Printf("节点%d将日志[%d]应用的状态机", rf.me, i)
		}
	}
}

// 一个定时任务，定期检查子节点的日志是否与当前leader节点的日志匹配情况，如果不匹配就发送数据到对应的节点
func (rf *Raft) copyLog() {
	for rf.killed() == false && rf.isRunningTask(&rf.runCopyLogTask) {
		// 100ms 进行一次日志复制
		time.Sleep(time.Duration(50) * time.Millisecond)
		rf.mu.Lock()
		isLeader := rf.state == LEADER
		rf.mu.Unlock()
		if isLeader == false {
			return
		}
		// 进行日志分发
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				serverId := i
				// 检查是否需要进行日志追加,提前判断，避免进行goroutine的创建处理
				rf.mu.Lock()
				lastLogIndex := len(rf.log) - 1
				needSend := lastLogIndex >= rf.nextIndex[serverId]
				rf.mu.Unlock()
				if needSend {
					go func() {
						// 重试次数
						args := &AppendEntriesArgs{}
						reply := &AppendEntriesReply{}
						ok, lastIndex := rf.sendAppendEntries(serverId, false, args, reply)
						if ok {
							rf.handleCopyLogResult(serverId, lastIndex, reply)
						} else {
							// 等待下一次任务执行
						}
					}()
				}
			}
		}
	}
}

func (rf *Raft) handleCopyLogResult(serverId int, lastIndex int, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Success == false {
		// 检查任期
		if reply.Term > rf.currentTerm {
			rf.changeState(FOLLOWER)
			rf.currentTerm = reply.Term
			return
		}
		// nextIndex 索引退避
		if rf.nextIndex[serverId] > 1 {
			rf.nextIndex[serverId]--
			return
		}
	} else {
		// 如果日志复制成功则更新matchIndex
		rf.matchIndex[serverId] = lastIndex
		rf.nextIndex[serverId] = lastIndex + 1
		log.Printf("LEADER %d更新节点%d的nextIndex为%d,matchIndex为%d\n",
			rf.me, serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
		// 需要寻找多数节点达成一致的地方
		for i := len(rf.log) - 1; i > 0 && i > rf.commitIndex; i-- {
			agreeNum := 0
			for j := 0; j < len(rf.peers); j++ {
				if rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
					agreeNum++
				}
			}
			if agreeNum > len(rf.peers)/2 {
				rf.commitIndex = i
				log.Printf("节点%d计算commitIndex成功，当前commitIndex=%d\n", rf.me, rf.commitIndex)
				break
			}
		}
		rf.applyLog()
	}
}

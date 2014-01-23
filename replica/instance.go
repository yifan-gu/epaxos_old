package replica

import (
	cmd "github.com/go-epaxos/epaxos/command"
)

const (
	preparing int8 = iota
	preAccepted
	accepted
	committed
	executed
)

// a bookkeeping for infos like maxBallot, # of nack, # of ok, etc
type InstanceInfo struct {
	preAcceptCount int
	isFastPath     bool

	acceptNackCount int
	acceptCount     int

	prepareCount int
}

type RecoveryInfo struct {
	preAcceptCount  int
	replyCount      int
	maxAcceptBallot *Ballot

	cmds   []cmd.Command
	deps   dependencies
	status int8
}

type Instance struct {
	cmds         []cmd.Command
	deps         dependencies
	status       int8
	ballot       *Ballot
	info         *InstanceInfo
	recoveryInfo *RecoveryInfo
}

func NewRecoveryInfo() *RecoveryInfo {
	return &RecoveryInfo{}
}

func NewInstanceInfo() *InstanceInfo {
	return &InstanceInfo{
		isFastPath: true,
	}
}

func NewInstance(cmds []cmd.Command, deps dependencies, status int8) *Instance {
	return &Instance{
		cmds:   cmds,
		deps:   deps,
		status: status,
		info:   NewInstanceInfo(),
	}
}

func (i *Instance) processPreAcceptReply(par *PreAcceptReply, quorum, fastQuorum int) int8 {
	i.info.preAcceptCount++

	if update := i.unionDeps(par.deps); !update {
		if i.preAcceptCount() > 1 {
			i.setFastPath(false)
		}
	}

	if i.preAcceptCount() >= quorum-1 && !i.isFastPath() {
		i.status = accepted
	}

	if i.preAcceptCount() == fastQuorum && i.isFastPath() {
		i.status = committed
	}

	return i.status
}

func (i *Instance) processAccept(a *Accept) bool {
	if i.isEqualOrAfterStatus(accepted) || a.ballot.Compare(i.ballot) < 0 {
		return false
	}

	i.status = accepted
	i.cmds = a.cmds
	i.deps = a.deps
	i.ballot = a.ballot
	return true
}

func (i *Instance) processAcceptReply(ar *AcceptReply, quorum int) bool {
	if i.isAfterStatus(accepted) {
		// we've already moved on, this reply is a delayed one
		// so just ignore it
		// TODO: maybe we can have some warning message here
		return false
	}
	i.info.acceptCount++
	if i.info.acceptCount >= quorum-1 {
		i.status = committed
		return true
	}
	return false
}

func (i *Instance) processCommit(c *Commit) bool {
	if i.isEqualOrAfterStatus(committed) { // ignore the message
		return false
	}

	i.cmds = c.cmds
	i.deps = c.deps
	i.status = committed
	return true
}

func (i *Instance) preAcceptCount() int {
	return i.info.preAcceptCount
}

func (i *Instance) isFastPath() bool {
	return i.info.isFastPath
}

func (i *Instance) setFastPath(ok bool) {
	i.info.isFastPath = ok
}

func (i *Instance) unionDeps(deps dependencies) bool {
	return i.deps.union(deps)
}

func (i *Instance) isAtStatus(status int8) bool {
	return i.status == status
}

func (i *Instance) isAfterStatus(status int8) bool {
	return i.status > status
}

func (i *Instance) isEqualOrAfterStatus(status int8) bool {
	return i.status >= status
}

func (i *Instance) processPrepareReplies(p *PrepareReply) {
	rInfo := i.recoveryInfo

	switch p.status {
	case accepted:
		rInfo.status = accepted

		// only record the most recent accepted instance
		if p.ballot.Compare(rInfo.maxAcceptBallot) > 0 {
			rInfo.maxAcceptBallot = p.ballot
			rInfo.cmds, rInfo.deps = p.cmds, p.deps
		}
	case preAccepted:
		if rInfo.status >= accepted {
			break
		}
		rInfo.status = preAccepted

		// if former leader commits on fast-path,
		// it will only send to fast-quroum,
		// so we can safely union all deps here.
		rInfo.deps.union(p.deps)
		rInfo.preAcceptCount++
	default:
		// receiver has no info about the instance
	}
	rInfo.replyCount++
}

func (i *Instance) processRecovery(quorumSize int) (status int8) {
	rInfo := i.recoveryInfo

	switch rInfo.status {
	case accepted:
		i.cmds, i.deps, i.status = rInfo.cmds, rInfo.deps, accepted
	case preAccepted:
		i.cmds, i.deps = rInfo.cmds, rInfo.deps
		if rInfo.preAcceptCount >= quorumSize { // N/2 + 1
			// sendAccept()
			i.status = accepted
			break
		}
		// sendPreAccept()
		i.status = preAccepted
	default:
		// no any info about instance
		// sendPreAccept(noop)
		i.status = preAccepted
		i.cmds, i.deps = nil, nil // TODO: no-op
		// [*] I forgot why we can't send accept noop here...
	}
	return i.status
}

package replica

import (
	"fmt"
)

var _ = fmt.Printf

func (r *Replica) sendPrepare(replicaId int, instanceId InstanceId, messageChan chan Message) {
	if r.InstanceMatrix[replicaId][instanceId] == nil {
		// TODO: we need to prepare an instance that doesn't exist.
		r.InstanceMatrix[replicaId][instanceId] = &Instance{
			// TODO:
			// Assumed no-op to be nil here.
			// we need to do more since state machine needs to know how to interpret it.
			cmds:   nil,
			deps:   make([]InstanceId, r.Size), // TODO: makeInitialDeps
			status: -1,                         // 'none' might be a conflicting name. We currenctly pick '-1' for it
			ballot: r.makeInitialBallot(),
			info:   NewInstanceInfo(),
		}
	}

	inst := r.InstanceMatrix[replicaId][instanceId]

	// clean preAccepCount
	inst.recoveryInfo = NewRecoveryInfo()
	if inst.status == preAccepted {
		inst.recoveryInfo.status = preAccepted
		inst.recoveryInfo.preAcceptCount = 1
	}

	inst.ballot.incNumber()
	inst.ballot.setReplicaId(r.Id)

	prepare := &Prepare{
		ballot:     inst.ballot,
		replicaId:  replicaId,
		instanceId: instanceId,
	}

	go func() {
		for i := 0; i < r.Size-1; i++ {
			messageChan <- prepare
		}
	}()
}

func (r *Replica) recvPrepare(pp *Prepare, messageChan chan Message) {
	inst := r.InstanceMatrix[pp.replicaId][pp.instanceId]
	if inst == nil {
		// reply PrepareReply with no-op and invalid status
		pr := &PrepareReply{
			ok:         true,
			ballot:     r.makeInitialBallot(),
			status:     -1,                         // TODO: hardcode, not a best approach
			deps:       make([]InstanceId, r.Size), // TODO: makeInitialDeps
			replicaId:  pp.replicaId,
			instanceId: pp.instanceId,
		}
		r.sendPrepareReply(pr, messageChan)
		return
	}

	// we have some info about the instance
	pr := &PrepareReply{
		status:     inst.status,
		replicaId:  pp.replicaId,
		instanceId: pp.instanceId,
		cmds:       inst.cmds,
		deps:       inst.deps,
		ballot:     inst.ballot,
	}

	// we won't have the same ballot
	if pp.ballot.Compare(inst.ballot) > 0 {
		pr.ok = true
		inst.ballot = pp.ballot
	} else {
		pr.ok = false
	}

	r.sendPrepareReply(pr, messageChan)
}

func (r *Replica) sendPrepareReply(pr *PrepareReply, messageChan chan Message) {
	go func() {
		messageChan <- pr
	}()
}

func (r *Replica) recvPrepareReply(p *PrepareReply, m chan Message) {
	inst := r.InstanceMatrix[p.replicaId][p.instanceId]

	if inst == nil {
		msg := fmt.Sprintf("shouldn't get here, replicaId = %d, instanceId = %d",
			p.replicaId, p.instanceId)
		panic(msg)
	}
	// once we receive a "commited" reply,
	// then we can leave preparing state and send commits.
	// even if we are not in "preparing", we can use this info
	// and start sending commit immediately
	if p.status == committed && inst.status < committed {
		inst.cmds, inst.deps, inst.status = p.cmds, p.deps, committed
		r.sendCommit(p.replicaId, p.instanceId, m)
		return
	}

	// ignore delayed messages or nacks
	if !inst.isAtStatus(preparing) || !p.ok {
		return
	}

	inst.processPrepareReplies(p)
	if inst.recoveryInfo.replyCount < r.QuorumSize()-1 {
		return
	}

	// now we have received enough relies
	status := inst.processRecovery(r.QuorumSize())
	switch status {
	case accepted:
		r.sendAccept(p.replicaId, p.instanceId, m)
	case preAccepted:
		r.sendPreAccept(inst.cmds, inst.deps, p.instanceId, m)
	}
}

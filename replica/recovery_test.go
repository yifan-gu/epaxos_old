package replica

import (
	"fmt"
	"reflect"
	"testing"

	cmd "github.com/go-epaxos/epaxos/command"
)

var _ = fmt.Printf

func TestSendPrepare(t *testing.T) {
	messageChan := make(chan Message)
	// send prepare
	r := startNewReplica(0, 5)
	r.sendPrepare(0, conflictNotFound+1, messageChan)
	if r.InstanceMatrix[0][conflictNotFound+1].status != preparing {
		t.Fatal("instance's status should be `preparing'")
	}

	// verify the prepare message for:
	// - incremented ballot number
	// - L.i instance
	for i := 0; i < r.Size-1; i++ {
		message := <-messageChan
		prepare := message.(*Prepare)
		if prepare.ballot.getNumber() != 1 {
			t.Fatal("expected ballot number to be incremented to 1")
		}
	}

	r.InstanceMatrix[0][conflictNotFound+1].cmds = []cmd.Command{
		cmd.Command("hello"),
	}

	r.sendPrepare(0, conflictNotFound+1, messageChan)
	if r.InstanceMatrix[0][conflictNotFound+1].cmds[0].Compare(cmd.Command("hello")) != 0 {
		t.Fatal("The cmds shouldn't be changed")
	}
	for i := 0; i < r.Size-1; i++ {
		message := <-messageChan
		prepare := message.(*Prepare)
		if prepare.ballot.getNumber() != 2 {
			t.Fatal("expected ballot number to be incremented to 2")
		}
	}

}

// Receivers have no info about the Instance in Prepare
// Test if they will accept the Prepare message
// PrepareReply should be ok with no-op
// Success: Accept the Prepares, Failure: Reject the Prepares
func TestRecvPrepareNoInstance(t *testing.T) {
	g, r, messageChan := recoveryTestSetup(5)
	r.sendPrepare(0, conflictNotFound+1, messageChan)
	var pp *Prepare
	for i := 1; i < r.Size; i++ {
		pp = (<-messageChan).(*Prepare)
		g[i].recvPrepare(pp, messageChan)
	}
	// receivers have no info about the instance, they should reply ok
	for i := 1; i < r.Size; i++ {
		pr := (<-messageChan).(*PrepareReply)
		if !reflect.DeepEqual(pr, &PrepareReply{
			ok:         true,
			ballot:     &Ballot{0, 0, uint8(i)}, // receiver's initial ballot
			status:     -1,
			cmds:       nil,
			deps:       make([]InstanceId, r.Size), // TODO: makeInitialDeps
			replicaId:  0,
			instanceId: conflictNotFound + 1,
		}) {
			t.Fatal("PrepareReply message error")
		}
	}
	testNoMessagesLeft(messageChan, t)
}

// Receivers have larger ballot
// Test if they will reject the Prepare message
// PrepareReply should be not ok, with their version of instances
// Success: Reject the Prepares, Failure: Accept the Prepares
func TestRecvPrepareReject(t *testing.T) {
	g, r, messageChan := recoveryTestSetup(5)
	r.sendPrepare(0, conflictNotFound+2, messageChan)

	for i := 1; i < r.Size; i++ {
		// create instance in receivers, and make larger ballots
		g[i].InstanceMatrix[0][conflictNotFound+2] = &Instance{
			status: accepted,
			cmds: []cmd.Command{
				cmd.Command("paxos"),
			},
			deps: []InstanceId{1, 0, 0, 0, 0},
			// ballot num == 2
			ballot: r.makeInitialBallot().getIncNumCopy().getIncNumCopy(),
		}
	}
	// recv Prepares
	for i := 1; i < r.Size; i++ {
		pp := (<-messageChan).(*Prepare)
		g[i].recvPrepare(pp, messageChan)
	}
	// test PrepareReplies
	for i := 1; i < r.Size; i++ {
		pr := (<-messageChan).(*PrepareReply)
		if !reflect.DeepEqual(pr, &PrepareReply{
			ok: false,
			// info of the receivers' instance
			status: accepted,
			cmds: []cmd.Command{
				cmd.Command("paxos"),
			},
			deps: []InstanceId{1, 0, 0, 0, 0},
			// ballot num == 2
			ballot:     r.makeInitialBallot().getIncNumCopy().getIncNumCopy(), // receiver's ballot
			replicaId:  0,
			instanceId: conflictNotFound + 2,
		}) {
			t.Fatal("PrepareReply message error")
		}
	}
	testNoMessagesLeft(messageChan, t)
}

// Receivers have smaller ballot
// Test if they will accepte the Prepare message
// PrepareReply should be ok, with their version of instance
// Success: Accept the Prepares, Failure: Reject the Prepares
func TestRecvPrepareAccept(t *testing.T) {
	g, r, messageChan := recoveryTestSetup(5)
	r.sendPrepare(0, conflictNotFound+2, messageChan)

	// create instance in receivers, and make smaller ballots
	for i := 1; i < r.Size; i++ {
		g[i].InstanceMatrix[0][conflictNotFound+2] = &Instance{
			status: accepted,
			cmds: []cmd.Command{
				cmd.Command("paxos"),
			},
			deps:   []InstanceId{1, 0, 0, 0, 0},
			ballot: r.makeInitialBallot(),
		}
	}
	// recv Prepares
	for i := 1; i < r.Size; i++ {
		pp := (<-messageChan).(*Prepare)
		g[i].recvPrepare(pp, messageChan)
	}
	// test PrepareReplies
	for i := 1; i < r.Size; i++ {
		pr := (<-messageChan).(*PrepareReply)
		if !reflect.DeepEqual(pr, &PrepareReply{
			ok: true,
			// info of the receivers' instance
			status: accepted,
			cmds: []cmd.Command{
				cmd.Command("paxos"),
			},
			deps:       []InstanceId{1, 0, 0, 0, 0},
			ballot:     r.makeInitialBallot(), // receiver's initial ballot
			replicaId:  0,
			instanceId: conflictNotFound + 2,
		}) {
			t.Fatal("PrepareReply message error")
		}
	}
	testNoMessagesLeft(messageChan, t)
}

func TestRecvPrepareReply(t *testing.T) {
	// A lot of work...
}

// In this test, the sender of the Prepare will receive replis contains Commit,
// so the sender of should send one round of Commits
// Success: Get one round of Commits messages, Failure: Otherwise
func TestRecvPrepareReplyCommit(t *testing.T) {
	g, r, messageChan := recoveryTestSetup(5)
	r.sendPrepare(1, conflictNotFound+2, messageChan)

	// create instance in receivers, and make smaller ballots
	for i := 1; i < r.Size; i++ {
		g[i].InstanceMatrix[1][conflictNotFound+2] = &Instance{
			status: committed,
			cmds: []cmd.Command{
				cmd.Command("paxos"),
			},
			deps:   []InstanceId{1, 0, 0, 0, 0},
			ballot: g[1].makeInitialBallot(),
		}
	}
	// recv Prepares
	for i := 1; i < r.Size; i++ {
		pp := (<-messageChan).(*Prepare)
		g[i].recvPrepare(pp, messageChan)
	}

	// recv PrepareReplies
	for i := 1; i < r.Size; i++ {
		pr := (<-messageChan).(*PrepareReply)
		r.recvPrepareReply(pr, messageChan)
	}

	// test if r sends Commits, (it should send only one round of Commits)
	for i := 0; i < r.Size-1; i++ {
		cm := (<-messageChan).(*Commit)
		if !reflect.DeepEqual(cm, &Commit{
			cmds: []cmd.Command{
				cmd.Command("paxos"),
			},
			deps:       []InstanceId{1, 0, 0, 0, 0},
			replicaId:  1,
			instanceId: conflictNotFound + 2,
		}) {
			t.Fatal("PrepareReply message error")
		}

	}
	testNoMessagesLeft(messageChan, t)
}

// In this test, the sender of the Prepare message will receive some replies contains Accepts,
// so after receiving N/2 replies, it should send the most recent Accept message
// Success: send one round of Accept messages, Failure: otherwise
func TestRecvPrepareReplyAccept(t *testing.T) {
	g, r, messageChan := recoveryTestSetup(10)

	r.InstanceMatrix[1][conflictNotFound+3] = NewInstance(nil, nil, 0)
	r.InstanceMatrix[1][conflictNotFound+3].ballot = r.makeInitialBallot().getIncNumCopy().getIncNumCopy()
	r.sendPrepare(1, conflictNotFound+3, messageChan)

	// create instance in receivers, and make smaller ballots
	g[1].InstanceMatrix[1][conflictNotFound+3] = &Instance{
		status: accepted,
		cmds: []cmd.Command{
			cmd.Command("paxos"),
		},
		deps:   []InstanceId{0, 1, 1, 0, 0},
		ballot: g[1].makeInitialBallot().getIncNumCopy(),
	}
	g[2].InstanceMatrix[1][conflictNotFound+3] = &Instance{
		status: accepted,
		cmds: []cmd.Command{
			cmd.Command("paxos"),
		},
		deps:   []InstanceId{0, 2, 0, 0, 0},
		ballot: g[1].makeInitialBallot(),
	}
	g[3].InstanceMatrix[1][conflictNotFound+3] = &Instance{
		status: accepted,
		cmds: []cmd.Command{
			cmd.Command("paxos"),
		},
		deps:   []InstanceId{0, 0, 0, 1, 0},
		ballot: g[1].makeInitialBallot(),
	}
	g[4].InstanceMatrix[1][conflictNotFound+3] = &Instance{
		status: accepted,
		cmds: []cmd.Command{
			cmd.Command("paxos"),
		},
		deps: []InstanceId{0, 0, 2, 0, 1},
		// [*] Notice here, the sender will pick up this copy of instance
		// since it has the largest valid ballot among receivers
		ballot: g[1].makeInitialBallot().getIncNumCopy().getIncNumCopy(),
	}

	// recv Prepares
	for i := 1; i < r.Size; i++ {
		pp := (<-messageChan).(*Prepare)
		g[i].recvPrepare(pp, messageChan)
	}

	// recv PrepareReplies
	for i := 1; i < r.Size; i++ {
		pr := (<-messageChan).(*PrepareReply)
		r.recvPrepareReply(pr, messageChan)
	}

	// test if r sends Accepts, (it should send only one round of Accepts)
	for i := 0; i < r.QuorumSize()-1; i++ {
		ac := (<-messageChan).(*Accept)
		// test if the sender send the right Accept message
		if !reflect.DeepEqual(ac, &Accept{
			cmds: []cmd.Command{
				cmd.Command("paxos"),
			},
			deps:       []InstanceId{0, 0, 2, 0, 1}, // same as g[4]'s copy of instance
			replicaId:  1,
			instanceId: conflictNotFound + 3,
			ballot:     r.makeInitialBallot().getIncNumCopy().getIncNumCopy().getIncNumCopy(),
		}) {
			t.Fatal("PrepareReply message error")
		}

	}
	testNoMessagesLeft(messageChan, t)
}

// helpers
func recoveryTestSetup(size int) ([]*Replica, *Replica, chan Message) {
	g := testMakeRepGroup(size)
	messageChan := make(chan Message, 100)
	return g, g[0], messageChan
}

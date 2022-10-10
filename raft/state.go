package raft

import (
	"errors"
	"fmt"
	"time"
)

type State uint8

const (
	leader State = iota
	follower
	candidate
	dead
)

var States = [...]string{
	"leader", "follower", "candidate",
}

func (s *RaftServer) becomeFollower(term int32) {
	s.state = follower
	s.currentTerm = term
	s.votedFor = ""
	s.lastHeartbeat = time.Now()
	fmt.Println("becomeFollower: running election timer")
	go s.runElectionTimer()
}

func (s *RaftServer) becomeCandidate() error {
	if s.state == leader {
		return errors.New("current state is 'leader', leaders cannot become candidates")
	} else {
		s.state = candidate
	}
	return nil
}

func (s *RaftServer) becomeLeader() {
	if s.state != candidate {
		s.errC <- fmt.Errorf("only candidates can become leaders, current state is '%v'", s.state)
	} else {
		s.state = leader
	}

	for _, peer := range s.quorum {
		s.nextIndex[peer.Id] = len(s.log)
		s.matchIndex[peer.Id] = -1
	}

	// TODO: separate go routine into it's own function
	go func() {
		ticker := NewTicker(50 * time.Millisecond)
		defer ticker.timeout.Stop()

		println("Sending heartbeats as leader")
		for {
			s.sendHeartbeat()
			<-ticker.ch

			s.Lock()
			if s.state != leader {
				s.Unlock()
				println("State no longer leader")
				return
			}
			s.Unlock()
		}
	}()
}

package raft

import (
	"errors"
	"fmt"
)

type State uint8

const (
	leader State = iota
	follower
	candidate
)

func (s *RaftServer) becomeFollower() {
	s.state = follower
}

func (s *RaftServer) becomeCandidate() error {
	if s.state == leader {
		return errors.New("current state is 'leader', leaders cannot become candidates")
	} else {
		s.state = candidate
	}
	return nil
}

func (s *RaftServer) becomeLeader() error {
	if s.state != candidate {
		return fmt.Errorf("only candidates can become leaders, current state is '%v'", s.state)
	} else {
		s.state = leader
	}
	return nil
}

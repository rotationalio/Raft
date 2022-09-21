package raft

import (
	"fmt"
	"math/rand"
	"time"
)

type Ticker struct {
	period  time.Duration
	timeout *time.Ticker
	ch      <-chan time.Time
}

func (t *Ticker) Reset(period time.Duration) {
	t.timeout.Reset(period)
}

func NewTicker(period time.Duration) *Ticker {
	timeout := *time.NewTicker(period)
	return &Ticker{period: period, timeout: &timeout, ch: timeout.C}
}

func (s *RaftServer) runElectionTimer() {
	s.Lock()
	startingTerm := s.currentTerm
	s.Unlock()

	timeout := getElectionTick()
	ticker := NewTicker(10 * time.Millisecond)
	defer ticker.timeout.Stop()

	for {
		<-ticker.ch

		s.Lock()
		if s.state == leader {
			s.Unlock()
			fmt.Println("election timer running while in leader state")
			return
		}

		if startingTerm != s.currentTerm {
			s.Unlock()
			fmt.Println("term incremented while election timer running")
			return
		}

		if time.Since(s.lastHeartbeat) >= timeout {
			s.startElection()
			s.Unlock()
			return
		}
		s.Unlock()
	}
}

func getElectionTick() time.Duration {
	timeout := rand.Intn(500)
	randomTimeout := time.Duration(1000+timeout) * time.Millisecond
	return randomTimeout
}

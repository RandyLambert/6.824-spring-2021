package raft_test

import (
	"testing"
	"time"
)

func TestTicker(t *testing.T) {

	for {
		timeOut := time.After(2 * time.Second)
		tickTimer := time.NewTicker(1 * time.Second)
		barTimer := time.NewTicker(2 * time.Second)
		select {
		case <-tickTimer.C:
			t.Log("tick")
		case <-barTimer.C:
			t.Log("bar")
		case <-timeOut:
			t.Log("test")
		}
	}
}

func TestHelloWorld(t *testing.T) {
	var s []string
	//s := make([]string,5)
	t.Log(len(s))

	s = append(s,"sdada")
	s = append(s,"sdada")
	s = append(s,"sdada")
	s = append(s,"sdada")
	s = append(s,"sdada")

	t.Log(len(s))

	s = s[0:3]

	t.Log(len(s))
}

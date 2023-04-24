package scheduler

import (
	"fmt"
	"testing"
	"time"
)

func TestTask(t *testing.T) {
	c1 := &TaskConfig{
		Name:     "task1",
		Interval: 10,
	}
	err := RegisterTask(&task1{}, c1)
	if err != nil {
		panic(err)
	}
	c2 := &TaskConfig{
		Name: "task2",
		Cron: "0/10 * * * * ?",
	}
	err = RegisterTask(&task2{}, c2)
	if err != nil {
		panic(err)
	}
	Start()
	for {
		select {
		case <-time.NewTimer(time.Minute).C:
			return
		}
	}
}

type task1 struct {
}

func (t *task1) Name() string {
	return "task1"
}
func (t *task1) Run() {
	fmt.Println(t.Name(), "run", time.Now().Format("2006-01-02 15:04:05"))
}

func (t *task1) Init(_ map[string]interface{}) error {
	fmt.Println(t.Name(), "init", time.Now().Format("2006-01-02 15:04:05"))
	return nil
}

type task2 struct {
}

func (t *task2) Name() string {
	return "task2"
}
func (t *task2) Run() {
	fmt.Println(t.Name(), "run", time.Now().Format("2006-01-02 15:04:05"))
}

func (t *task2) Init(_ map[string]interface{}) error {
	fmt.Println(t.Name(), "init", time.Now().Format("2006-01-02 15:04:05"))
	return nil
}

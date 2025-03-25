package scheduler

import (
	"fmt"
	"strings"
	"time"

	"github.com/basebytes/types"
)

const (
	OpStart   = "start"
	OpStop    = "stop"
	OpAdd     = "add"
	OpCancel  = "cancel"
	OpUpdate  = "update"
	OpPause   = "pause"
	OpResume  = "resume"
	OpIgnore  = "ignore"
	OpExecute = "execute"
)

type Option func(op *Operate)

func withPlan(plan string) Option {
	return func(op *Operate) {
		op.Plan = plan
	}
}

func withJob(job string) Option {
	return func(op *Operate) {
		op.Job = job
	}
}

func withPlanTime(plan *types.Time) Option {
	return func(op *Operate) {
		if plan != nil {
			op.PlanTime = plan
		}
	}
}

func withStartTime(start *types.Time) Option {
	return func(o *Operate) {
		if start != nil {
			o.StartTime = start
		}
	}
}

func withCode(code byte) Option {
	return func(op *Operate) {
		op.Code = code
	}
}

func newOperate(task, op string, start *types.Time, opts ...Option) *Operate {
	o := &Operate{
		Task:    task,
		Op:      op,
		EndTime: types.Now(),
	}
	opts = append(opts, withStartTime(start))
	for _, opt := range opts {
		opt(o)
	}
	return o
}

type Operate struct {
	Task      string      `json:"task,omitempty"`
	Plan      string      `json:"plan,omitempty"`
	Job       string      `json:"job,omitempty"`
	Op        string      `json:"op,omitempty"`
	PlanTime  *types.Time `json:"planTime,omitempty"`
	StartTime *types.Time `json:"startTime,omitempty"`
	EndTime   *types.Time `json:"endTime,omitempty"`
	Code      byte        `json:"code,omitempty"`
}

func (op *Operate) String() string {
	line := make([]string, 0, 18)
	line = append(line, op.StartTime.Format("15:04:05"))
	if op.EndTime.Sub(op.StartTime.Time) > time.Second {
		line = append(line, "-", op.EndTime.Format("15:04:05"))
	} else {
		line = append(line, "\t\t")
	}
	line = append(line, "\ttask[", op.Task, "]")
	if op.Plan != "" {
		line = append(line, " plan[", op.Plan, "]")
	}
	if op.Job != "" {
		line = append(line, " job[", op.Job, "]")
	}
	line = append(line, " operate[", op.Op, "]")
	if op.Op == OpIgnore {
		line = append(line, " code[", fmt.Sprintf("%d", op.Code), "]")
	}
	return strings.Join(line, "")
}

const (
	_ byte = iota
	TaskNotFound
	TaskIsBusy
	PlanNotFound
	PlanStop
	TooManyJob
	MaxDelay
)

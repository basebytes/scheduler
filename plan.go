package scheduler

import (
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"sync/atomic"

	"github.com/basebytes/types"
	"github.com/robfig/cron/v3"
)

const (
	Idle int32 = iota
	Running
	Pause
	Waiting
	Cancel
	Stopped
)

type PlanConfig struct {
	key      string
	Cron     string          `json:"cron,omitempty"`
	Interval *types.Duration `json:"interval,omitempty"`
	StopAt   *types.Time     `json:"stopAt,omitempty"`
}

func (cfg *PlanConfig) Check() (err error) {
	cfg.Cron = strings.TrimSpace(cfg.Cron)
	if cfg.Cron == "" && (cfg.Interval == nil || cfg.Interval.Duration <= 0) {
		err = ExecuteParamNotFoundErr
	} else if cfg.Cron != "" && cfg.Interval != nil && cfg.Interval.Duration > 0 {
		err = ExecuteParamConflictErr
	}
	if cfg.Interval != nil {
		if cfg.Interval.Duration < 0 {
			cfg.Interval = nil
		}
	}
	return
}

func (cfg *PlanConfig) Key() string {
	if cfg.key == "" {
		cfg.key = fmt.Sprintf("%x", crc32.ChecksumIEEE([]byte(fmt.Sprintf("cron:%s;interval:%s", cfg.Cron, cfg.Interval))))
	}
	return cfg.key
}

func newPlan(code string, config *PlanConfig, execute executeFunc) *plan {
	return &plan{
		code:    code,
		key:     config.Key(),
		stopAt:  config.StopAt,
		execute: execute,
	}
}

type plan struct {
	key     string
	id      cron.EntryID
	status  atomic.Int32
	stopAt  *types.Time
	code    string
	execute executeFunc
	stop    func(string)
}

func (p *plan) Run() {
	now := types.Now()
	if stopAt := p.stopAt; stopAt != nil && !now.Before(stopAt.Time) {
		go p.stop(p.key)
		return
	}
	if p.status.CompareAndSwap(Idle, Running) {
		p.execute(p.code, p.key, now, p.stopAt)
		p.status.CompareAndSwap(Running, Idle)
	}
}

func (p *plan) pause() {
	for p.status.CompareAndSwap(Idle, Pause) {
	}
}

func (p *plan) resume() {
	p.status.CompareAndSwap(Pause, Idle)
}

func (p *plan) planId(planId cron.EntryID) *plan {
	p.id = planId
	return p
}

func (p *plan) stopFunc(stop func(string)) *plan {
	p.stop = stop
	return p
}

func (p *plan) Key() string {
	return p.key
}

func (p *plan) ID() cron.EntryID {
	return p.id
}

func (p *plan) Status() int32 {
	return p.status.Load()
}

var (
	ExecuteParamNotFoundErr = errors.New("job execute parameter cron or interval is not specified")
	ExecuteParamConflictErr = errors.New("can not specify both cron and interval")
)

package scheduler

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

func newJobs() *jobs {
	return &jobs{}
}

type jobs map[string]*job

func (j *jobs) AddJob(_job *job) {
	(*j)[_job.id] = _job
}

func (j *jobs) removeJob(id string) {
	delete(*j, id)
}

func (j *jobs) GetJob(id string) *job {
	return (*j)[id]
}

func newJob(plan string) *job {
	return (&job{plan: plan}).init()
}

type job struct {
	id        string
	plan      string
	status    atomic.Int32
	cancelJob context.CancelFunc
}

func (j *job) init() *job {
	j.id = fmt.Sprintf("J%s", time.Now().Format("150405"))
	j.status.Store(Waiting)
	return j
}

func (j *job) ID() string {
	return j.id
}

func (j *job) Plan() string {
	return j.plan
}

func (j *job) Status() int32 {
	return j.status.Load()
}

func (j *job) swap(old, new int32) bool {
	return j.status.CompareAndSwap(old, new)
}

func (j *job) setCancel(cancel context.CancelFunc) {
	j.cancelJob = cancel
}

func (j *job) cancel() {
	if j.status.CompareAndSwap(Waiting, Cancel) {
		j.cancelJob()
	}
}

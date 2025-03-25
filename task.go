package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basebytes/types"
)

type Task interface {
	Code() string
	Run(base time.Time)
	Init(map[string]any) error
}

type TaskConfig struct {
	Code          string          `json:"code,omitempty"`
	Params        map[string]any  `json:"params,omitempty"`
	MaxDelay      *types.Duration `json:"maxDelay,omitempty"`
	MaxWaitingJob int32           `json:"maxWaitingJob,omitempty"`
	Plans         []*PlanConfig   `json:"plans,omitempty"`
}

func (cfg *TaskConfig) Check() (err error) {
	if cfg.Code == "" {
		err = TaskCodeNotFoundErr
	} else if len(cfg.Plans) == 0 {
		err = PlanConfigNotFoundErr
	} else {
		for _, _plan := range cfg.Plans {
			if err = _plan.Check(); err != nil {
				break
			}
		}
	}
	return
}

func (cfg *TaskConfig) AddParam(key string, value any) {
	if cfg.Params == nil {
		cfg.Params = make(map[string]any)
	}
	cfg.Params[key] = value
}

func (cfg *TaskConfig) SetDefaults(defaults map[string]any) {
	if defaults == nil || len(defaults) == 0 {
		return
	}
	if cfg.Params == nil {
		cfg.Params = make(map[string]any)
	}
	for k, v := range defaults {
		if _, ok := cfg.Params[k]; !ok {
			cfg.Params[k] = v
		}
	}
}

func NewTaskWrapper(task Task, maxDelay *types.Duration, maxWaitingJob int32, cancelFunc cancelFunc, shouldStop stopFunc, logger OpLogger) *taskWrapper {
	t := &taskWrapper{task: task, maxDelay: maxDelay, maxWaitingJob: maxWaitingJob, cancelPlanFunc: cancelFunc, shouldStopPlan: shouldStop, logger: logger}
	return t.init()
}

type taskWrapper struct {
	task           Task
	maxDelay       *types.Duration
	maxWaitingJob  int32
	jobCount       atomic.Int32
	plans          map[string]*plan
	jobs           map[string]*jobs
	lock           sync.RWMutex
	status         atomic.Int32
	ctx            context.Context
	cancel         context.CancelFunc
	cancelPlanFunc cancelFunc
	shouldStopPlan stopFunc
	logger         OpLogger
}

func (t *taskWrapper) init() *taskWrapper {
	t.plans = make(map[string]*plan)
	t.jobs = make(map[string]*jobs)
	t.ctx, t.cancel = context.WithCancel(context.Background())
	return t
}

func (t *taskWrapper) AddPlan(plan *plan) (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	key := plan.Key()
	if _, ok := t.plans[key]; ok {
		err = PlanExistErr
	} else {
		t.plans[key] = plan.stopFunc(t.stopPlan)
	}
	return
}

func (t *taskWrapper) Cancel() {
	t.lock.Lock()
	defer func(start *types.Time) {
		t.logger(newOperate(t.Code(), OpCancel, start))
		t.lock.Unlock()
	}(types.Now())
	for _, _plan := range t.plans {
		t.cancelPlan(_plan, OpCancel)
	}
	for _, _jobs := range t.jobs {
		for _, _job := range *_jobs {
			_job.swap(Waiting, Cancel)
		}
	}
	t.cancel()
}

func (t *taskWrapper) JobCount() int32 {
	return t.jobCount.Load()
}

func (t *taskWrapper) Pause() {
	t.lock.Lock()
	defer func(start *types.Time) {
		t.logger(newOperate(t.Code(), OpPause, start))
		t.lock.Unlock()
	}(types.Now())
	for _, _plan := range t.plans {
		t.pausePlan(_plan)
	}
}

func (t *taskWrapper) Resume() {
	t.lock.Lock()
	defer func(start *types.Time) {
		t.logger(newOperate(t.Code(), OpResume, start))
		t.lock.Unlock()
	}(types.Now())
	for _, _plan := range t.plans {
		t.resumePlan(_plan)
	}
}

func (t *taskWrapper) UpdateTask(task Task) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.task.Code() == task.Code() {
		t.task = task
	}
}

func (t *taskWrapper) CancelPlan(key string) (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if _plan, ok := t.plans[key]; ok {
		t.cancelPlan(_plan, OpCancel)
		t.cancelPlanJobs(key)
		delete(t.plans, key)
	} else {
		err = PlanNotFoundErr
	}
	return
}

func (t *taskWrapper) PausePlan(key string) (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if _plan, ok := t.plans[key]; ok {
		t.pausePlan(_plan)
	} else {
		err = PlanNotFoundErr
	}
	return
}

func (t *taskWrapper) ResumePlan(key string) (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if _plan, ok := t.plans[key]; ok {
		t.resumePlan(_plan)
	} else {
		err = PlanNotFoundErr
	}
	return
}

func (t *taskWrapper) ExistPlan(key string) (ok bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	_, ok = t.plans[key]
	return
}

func (t *taskWrapper) CancelJob(key string, jobIds ...string) (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if _, ok := t.plans[key]; ok {
		if _jobs := t.jobs[key]; _jobs != nil {
			for _, jobId := range jobIds {
				if _job := _jobs.GetJob(jobId); _job != nil {
					go _job.cancel()
				}
			}
		}
	} else {
		err = PlanNotFoundErr
	}
	return
}

func (t *taskWrapper) Code() string {
	return t.task.Code()
}

func (t *taskWrapper) Status() int32 {
	return t.status.Load()
}

func (t *taskWrapper) PlanStatus(plans ...string) map[string]int32 {
	t.lock.RLock()
	defer t.lock.RUnlock()
	results := make(map[string]int32, len(t.plans))
	if len(plans) == 0 {
		for key, _plan := range t.plans {
			results[key] = _plan.Status()
		}
	} else {
		for _, key := range plans {
			if _plan, ok := t.plans[key]; ok {
				results[key] = _plan.Status()
			}
		}
	}
	return results
}

func (t *taskWrapper) JobStatus(plans ...string) map[string]map[string]int32 {
	t.lock.RLock()
	defer t.lock.RUnlock()
	result := make(map[string]map[string]int32, len(t.jobs))
	if len(plans) == 0 {
		for key := range t.jobs {
			result[key] = t.planJobStatus(key)
		}
	} else {
		for _, key := range plans {
			result[key] = t.planJobStatus(key)
		}
	}
	return result
}

func (t *taskWrapper) PlanJobStatus(plan string, jobIds ...string) map[string]int32 {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.planJobStatus(plan, jobIds...)
}

func (t *taskWrapper) planJobStatus(plan string, jobIds ...string) map[string]int32 {
	result := make(map[string]int32, t.JobCount())
	if _jobs, ok := t.jobs[plan]; ok {
		if len(jobIds) == 0 {
			for id, _job := range *_jobs {
				result[id] = _job.Status()
			}
		} else {
			for _, id := range jobIds {
				if _job := _jobs.GetJob(id); _job != nil {
					result[id] = _job.Status()
				}
			}
		}
	}
	return result
}

func (t *taskWrapper) execute(key string, baseTime, stopAt *types.Time, maxDelay *types.Duration) (jobId string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	planTime := types.Now()
	_job := newJob(key)
	if _, ok := t.plans[key]; ok || key == PlanManual {
		if _, ok = t.jobs[key]; !ok {
			t.jobs[key] = newJobs()
		}
		t.jobs[key].AddJob(_job)
		if maxDelay == nil || maxDelay.Duration == 0 {
			go t.quickEnd(_job, planTime, baseTime, stopAt)
		} else {
			go t.waitForExecute(_job, planTime, baseTime, stopAt, maxDelay)
		}
		jobId = _job.ID()
	} else {
		t.logger(newOperate(t.Code(), OpIgnore, planTime, withPlan(key), withJob(_job.ID()), withPlanTime(planTime), withCode(PlanNotFound)))
	}
	return
}

func (t *taskWrapper) waitForExecute(_job *job, planTime, baseTime, stopAt *types.Time, maxDelay *types.Duration) {
	var (
		ticker        = time.NewTicker(time.Second)
		ctx           context.Context
		cancelJobFunc context.CancelFunc
	)
	if maxDelay != nil && maxDelay.Duration > 0 {
		ctx, cancelJobFunc = context.WithDeadline(t.ctx, planTime.Add(maxDelay.Duration))
	} else {
		ctx, cancelJobFunc = context.WithCancel(t.ctx)
	}
	_job.setCancel(cancelJobFunc)
	defer func() {
		ticker.Stop()
		t.removeJob(_job, planTime)
		t.jobCount.Add(-1)
	}()
	count := t.jobCount.Add(1)
	if t.maxWaitingJob > 0 && count > t.maxWaitingJob {
		t.logger(newOperate(t.Code(), OpIgnore, types.Now(), withPlan(_job.Plan()), withJob(_job.ID()), withPlanTime(planTime), withCode(TooManyJob)))
		return
	}
	for !t.status.CompareAndSwap(Idle, Running) {
		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			if _job.Status() == Waiting {
				t.logger(newOperate(t.Code(), OpIgnore, types.Now(), withPlan(_job.Plan()), withJob(_job.ID()), withPlanTime(planTime), withCode(MaxDelay)))
			}
			return
		}
	}
	t.run(_job, planTime, baseTime, stopAt)
}

func (t *taskWrapper) quickEnd(_job *job, planTime, baseTime, stopAt *types.Time) {
	defer t.removeJob(_job, planTime)
	if t.status.CompareAndSwap(Idle, Running) {
		t.run(_job, planTime, baseTime, stopAt)
	} else {
		t.logger(newOperate(t.Code(), OpIgnore, planTime, withPlan(_job.Plan()), withJob(_job.ID()), withPlanTime(planTime), withCode(TaskIsBusy)))
	}
}

func (t *taskWrapper) run(_job *job, planTime, baseTime, stopAt *types.Time) {
	if _job.swap(Waiting, Running) {
		start := types.Now()
		t.task.Run(baseTime.Time)
		t.logger(newOperate(t.Code(), OpExecute, start, withPlan(_job.Plan()), withJob(_job.ID()), withPlanTime(planTime)))
		if stopAt != nil {
			_plan := t.getPlan(_job.Plan())
			if t.shouldStopPlan(_plan.ID(), stopAt) {
				t.stopPlan(_job.Plan())
			}
		}
		t.status.CompareAndSwap(Running, Idle)
	}
}

func (t *taskWrapper) stopPlan(key string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if _plan, ok := t.plans[key]; ok {
		t.cancelPlan(_plan, OpStop)
		t.cancelPlanJobs(key)
		delete(t.plans, key)
	}
}

func (t *taskWrapper) removeJob(_job *job, planTime *types.Time) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if _jobs, ok := t.jobs[_job.Plan()]; ok {
		_jobs.removeJob(_job.ID())
		if _job.Status() == Cancel {
			t.logger(newOperate(t.Code(), OpCancel, types.Now(), withPlan(_job.Plan()), withJob(_job.ID()), withPlanTime(planTime)))
		}
	}
}

func (t *taskWrapper) cancelPlan(plan *plan, op string) {
	defer func(start *types.Time) {
		t.logger(newOperate(t.Code(), op, start, withPlan(plan.Key())))
	}(types.Now())
	t.cancelPlanFunc(plan.ID())
	plan.status.Store(Cancel)
}

func (t *taskWrapper) pausePlan(plan *plan) {
	defer func(start *types.Time) {
		t.logger(newOperate(t.Code(), OpPause, start, withPlan(plan.Key())))
	}(types.Now())
	plan.pause()
}

func (t *taskWrapper) resumePlan(plan *plan) {
	defer func(start *types.Time) {
		t.logger(newOperate(t.Code(), OpResume, start, withPlan(plan.Key())))
	}(types.Now())
	plan.resume()
}

func (t *taskWrapper) cancelPlanJobs(key string) {
	if _jobs := t.jobs[key]; _jobs != nil {
		for _, _job := range *_jobs {
			go _job.cancel()
		}
	}
}

func (t *taskWrapper) getPlan(key string) *plan {
	t.lock.RLock()
	defer t.lock.RUnlock()
	if _plan, ok := t.plans[key]; ok {
		return _plan
	}
	return nil
}

var (
	TaskNotFoundErr     = errors.New("task not found")
	TaskCodeNotFoundErr = errors.New("task for specified name not found")
	TaskExistErr        = errors.New("task already exist")
	PlanNotFoundErr       = errors.New("plan not found")
	PlanExistErr          = errors.New("plan already exist")
	PlanConfigNotFoundErr = errors.New("plan config not found")
)

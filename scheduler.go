package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/basebytes/types"
	"github.com/robfig/cron/v3"
)

type OpLogger func(op *Operate)

func WithOpLogger(l OpLogger) {
	scheduler.WithOpLogger(l)
}

func Start() {
	scheduler.Start()
}

func Stop() context.Context {
	return scheduler.Stop()
}

func RegisterTask(task Task, cfg *TaskConfig) error {
	return scheduler.RegisterTask(task, cfg)
}

func CancelTask(code string) context.Context {
	return scheduler.CancelTask(code)
}

func PauseTask(code string) {
	scheduler.PauseTask(code)
}

func ResumeTask(code string) {
	scheduler.ResumeTask(code)
}

func UpdateTask(task Task, params map[string]any) error {
	return scheduler.UpdateTask(task, params)
}

func ExecuteTask(code string, base *types.Time, maxDelay *types.Duration) (string, error) {
	return scheduler.ExecuteTask(code, base, maxDelay)
}

func ExistTask(code string) bool {
	return scheduler.ExistTask(code)
}

func AddPlan(code string, cfg *PlanConfig) error {
	return scheduler.AddPlan(code, cfg)
}

func CancelPlan(code, plan string) error {
	return scheduler.CancelPlan(code, plan)
}

func PausePlan(code, plan string) error {
	return scheduler.PausePlan(code, plan)
}

func ResumePlan(code, plan string) error {
	return scheduler.ResumePlan(code, plan)
}

func ExistPlan(code, plan string) bool {
	return scheduler.ExistPlan(code, plan)
}

func CancelJob(code, plan string, jobIds ...string) error {
	return scheduler.CancelJob(code, plan, jobIds...)
}

func TaskStatus(codes ...string) map[string]int32 {
	return scheduler.TaskStatus(codes...)
}

func PlanStatus(code ...string) map[string]map[string]int32 {
	return scheduler.PlanStatus(code...)
}

func JobStatus(code ...string) map[string]map[string]map[string]int32 {
	return scheduler.JobStatus(code...)
}

func TaskPlanStatus(code string, plan ...string) map[string]int32 {
	return scheduler.TaskPlanStatus(code, plan...)
}

func TaskJobStatus(code string, plans ...string) map[string]map[string]int32 {
	return scheduler.TaskJobStatus(code, plans...)
}

func TaskPlanJobStatus(code, plan string, jobIds ...string) map[string]int32 {
	return scheduler.TaskPlanJobStatus(code, plan, jobIds...)
}

var (
	scheduler    = newScheduler()
	secondParser = cron.NewParser(
		cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)
)

func newScheduler() *Scheduler {
	return &Scheduler{
		cron:    cron.New(cron.WithParser(secondParser)),
		taskMap: make(map[string]*taskWrapper),
	}
}

type Scheduler struct {
	lock    sync.RWMutex
	cron    *cron.Cron
	logger  OpLogger
	taskMap map[string]*taskWrapper
	running bool
}

func (s *Scheduler) WithOpLogger(l OpLogger) {
	s.logger = l
}

func (s *Scheduler) Start() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.running = true
	s.cron.Start()
}

func (s *Scheduler) Stop() context.Context {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.running = false
	var (
		waiter      sync.WaitGroup
		ctx, cancel = context.WithCancel(context.Background())
	)
	waiter.Add(1)
	taskMap := s.taskMap
	var jobCount int32
	for _, task := range s.taskMap {
		jobCount += task.JobCount()
		go task.Cancel()
	}
	s.cron.Stop()
	go func() {
		for jobCount > 0 {
			time.Sleep(500 * time.Millisecond)
			jobCount = 0
			for _, task := range taskMap {
				jobCount += task.JobCount()
			}
		}
		waiter.Done()
		s.lock.Lock()
		defer s.lock.Unlock()
		s.taskMap = make(map[string]*taskWrapper)
	}()
	go func() {
		waiter.Wait()
		cancel()
	}()
	return ctx
}

func (s *Scheduler) RegisterTask(task Task, cfg *TaskConfig) (err error) {
	if err = cfg.Check(); err == nil {
		if err = task.Init(cfg.Params); err == nil {
			s.lock.Lock()
			defer s.lock.Unlock()
			if _, OK := s.taskMap[task.Code()]; OK {
				err = TaskExistErr
			} else {
				err = s.addPlans(NewTaskWrapper(task, cfg.MaxDelay, cfg.MaxWaitingJob, s.cron.Remove, s.shouldStop, s.doLog), true, cfg.Plans...)
			}
		}
	}
	return
}

func (s *Scheduler) CancelTask(code string) context.Context {
	var (
		waiter      sync.WaitGroup
		ctx, cancel = context.WithCancel(context.Background())
		task, ok    = s.getTask(code)
	)
	if ok {
		waiter.Add(1)
		go task.Cancel()
		go func() {
			for task.JobCount() > 0 {
				time.Sleep(500 * time.Millisecond)
			}
			waiter.Done()
			s.removeTask(code)
		}()
	}
	go func() {
		waiter.Wait()
		cancel()
	}()
	return ctx
}

func (s *Scheduler) PauseTask(code string) {
	if task, ok := s.getTask(code); ok {
		task.Pause()
	}
}

func (s *Scheduler) ResumeTask(code string) {
	if task, ok := s.getTask(code); ok {
		task.Resume()
	}
}

func (s *Scheduler) UpdateTask(task Task, params map[string]any) (err error) {
	if err = task.Init(params); err == nil {
		start := types.Now()
		code := task.Code()
		if _task, ok := s.getTask(code); ok {
			_task.UpdateTask(task)
			s.doLog(newOperate(code, OpUpdate, start))
		} else {
			err = TaskNotFoundErr
		}
	}
	return
}

func (s *Scheduler) ExecuteTask(code string, baseTime *types.Time, maxDelay *types.Duration) (jobId string, err error) {
	if task, ok := s.getTask(code); ok {
		if baseTime == nil {
			baseTime = types.Now()
		}
		if s.isRunning() {
			jobId = task.execute(PlanManual, baseTime, nil, maxDelay)
		} else {
			err = StoppedErr
		}
	} else {
		err = TaskNotFoundErr
	}
	return
}

func (s *Scheduler) ExistTask(code string) (ok bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, ok = s.taskMap[code]
	return
}

func (s *Scheduler) AddPlan(code string, cfg *PlanConfig) (err error) {
	if task, ok := s.getTask(code); ok {
		s.lock.Lock()
		defer s.lock.Unlock()
		err = s.addPlans(task, false, cfg)
	} else {
		err = TaskNotFoundErr
	}
	return
}

func (s *Scheduler) CancelPlan(code, key string) (err error) {
	if task, ok := s.getTask(code); ok {
		err = task.CancelPlan(key)
	} else {
		err = TaskNotFoundErr
	}
	return
}

func (s *Scheduler) PausePlan(code, key string) (err error) {
	if task, ok := s.getTask(code); ok {
		err = task.PausePlan(key)
	} else {
		err = TaskNotFoundErr
	}
	return
}

func (s *Scheduler) ResumePlan(code, key string) (err error) {
	if task, ok := s.getTask(code); ok {
		err = task.ResumePlan(key)
	} else {
		err = TaskNotFoundErr
	}
	return
}

func (s *Scheduler) ExistPlan(code, key string) bool {
	if task, ok := s.getTask(code); ok {
		return task.ExistPlan(key)
	} else {
		return false
	}
}

func (s *Scheduler) CancelJob(code, key string, jobIds ...string) (err error) {
	if task, ok := s.getTask(code); ok {
		err = task.CancelJob(key, jobIds...)
	} else {
		err = TaskNotFoundErr
	}
	return
}

func (s *Scheduler) TaskStatus(codes ...string) map[string]int32 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	result := make(map[string]int32, len(s.taskMap))
	if len(codes) == 0 {
		for code, task := range s.taskMap {
			result[code] = task.Status()
		}
	} else {
		for _, code := range codes {
			if task, ok := s.taskMap[code]; ok {
				result[code] = task.Status()
			}
		}
	}
	return result
}

func (s *Scheduler) PlanStatus(codes ...string) map[string]map[string]int32 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	result := make(map[string]map[string]int32, len(s.taskMap))
	if len(codes) == 0 {
		for code, task := range s.taskMap {
			result[code] = task.PlanStatus()
		}
	} else {
		for _, code := range codes {
			if task, ok := s.taskMap[code]; ok {
				result[code] = task.PlanStatus()
			}
		}
	}
	return result
}

func (s *Scheduler) JobStatus(codes ...string) map[string]map[string]map[string]int32 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	result := make(map[string]map[string]map[string]int32, len(s.taskMap))
	if len(codes) == 0 {
		for code, task := range s.taskMap {
			result[code] = task.JobStatus()
		}
	} else {
		for _, code := range codes {
			if task, ok := s.taskMap[code]; ok {
				result[code] = task.JobStatus()
			}
		}
	}
	return result
}

func (s *Scheduler) TaskPlanStatus(code string, plans ...string) map[string]int32 {
	if task, ok := s.getTask(code); ok {
		return task.PlanStatus(plans...)
	}
	return nil
}

func (s *Scheduler) TaskJobStatus(code string, plans ...string) map[string]map[string]int32 {
	if task, ok := s.getTask(code); ok {
		return task.JobStatus(plans...)
	}
	return nil
}

func (s *Scheduler) TaskPlanJobStatus(code, plan string, jobIds ...string) map[string]int32 {
	if task, ok := s.getTask(code); ok {
		return task.PlanJobStatus(plan, jobIds...)
	}
	return nil
}

func (s *Scheduler) isRunning() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.running
}

func (s *Scheduler) addPlans(task *taskWrapper, newTask bool, configs ...*PlanConfig) (err error) {
	start := types.Now()
	var (
		newPlans = make(map[string]cron.Schedule, len(configs))
		code     = task.Code()
	)
	for _, cfg := range configs {
		key := cfg.Key()
		if _, ok := newPlans[key]; ok || key == PlanManual || task.ExistPlan(key) {
			err = fmt.Errorf("repeat plan config [%s]", key)
			break
		}
		var schedule cron.Schedule
		if cfg.Interval != nil && cfg.Interval.Duration > 0 {
			schedule = cron.Every(cfg.Interval.Duration)
		} else if schedule, err = secondParser.Parse(cfg.Cron); err != nil {
			break
		}
		if cfg.StopAt != nil && !schedule.Next(time.Now()).Before(cfg.StopAt.Time) {
			continue
		}
		newPlans[key] = schedule
	}
	if err == nil {
		if newTask && len(newPlans) == 0 {
			return
		}
		if newTask {
			s.taskMap[code] = task
			s.doLog(newOperate(code, OpAdd, start))
		}
		for _, cfg := range configs {
			key := cfg.Key()
			if schedule, ok := newPlans[key]; ok {
				_plan := newPlan(code, cfg, s.execute)
				_ = task.AddPlan(_plan.planId(s.cron.Schedule(schedule, _plan)))
				s.doLog(newOperate(code, OpAdd, start, withPlan(key)))
			} else {
				s.doLog(newOperate(code, OpStop, start, withPlan(key), withCode(PlanStop)))
			}
		}
	}
	return
}

func (s *Scheduler) execute(code, key string, baseTime, stopAt *types.Time) {
	if task, ok := s.getTask(code); ok {
		task.execute(key, baseTime, stopAt, task.maxDelay)
	} else {
		s.doLog(newOperate(code, OpIgnore, types.Now(), withPlan(key), withCode(TaskNotFound)))
	}
}

func (s *Scheduler) getTask(code string) (t *taskWrapper, ok bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	t, ok = s.taskMap[code]
	return
}

func (s *Scheduler) removeTask(code string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, ok := s.taskMap[code]; ok {
		delete(s.taskMap, code)
	}
	return
}

func (s *Scheduler) getEntry(planId cron.EntryID) *cron.Entry {
	s.lock.RLock()
	defer s.lock.RUnlock()
	entry := s.cron.Entry(planId)
	return &entry
}

func (s *Scheduler) shouldStop(planId cron.EntryID, stopAt *types.Time) bool {
	entry := s.getEntry(planId)
	return entry.ID == planId && !entry.Next.Before(stopAt.Time)
}

func (s *Scheduler) doLog(op *Operate) {
	if s.logger != nil {
		s.logger(op)
	}
}

type stopFunc func(planId cron.EntryID, stopAt *types.Time) bool
type cancelFunc func(planId cron.EntryID)
type executeFunc func(code string, plan string, baseTime, stopAt *types.Time)

const PlanManual = "manual"

var StoppedErr = errors.New("scheduler already stopped")

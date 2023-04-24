package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	taskMap map[string]cron.EntryID
	lock    sync.Mutex
	cron    *cron.Cron
}

func Start() {
	scheduler.cron.Start()
}

//TaskList Snapshot
func TaskList() (tasks []string) {
	for name := range scheduler.taskMap {
		tasks = append(tasks, name)
	}
	return
}

func RegisterTask(task Task, cfg *TaskConfig) error {
	return scheduler.RegisterTask(task, cfg)
}

func RemoveTask(name string) {
	scheduler.RemoveTask(name)
}

func Stop() context.Context {
	for name := range scheduler.taskMap {
		scheduler.RemoveTask(name)
	}
	return scheduler.cron.Stop()
}

func GetTask(name string) Task {
	return scheduler.GetTask(name)
}
func GetTaskStatus(name string) *TaskStatus {
	return scheduler.GetTaskStatus(name)
}
func Exist(name string) bool {
	return scheduler.Exist(name)
}

var scheduler = newScheduler()

func newScheduler() *Scheduler {
	return &Scheduler{
		taskMap: make(map[string]cron.EntryID),
		cron:    cron.New(cron.WithSeconds()),
	}
}

func (s *Scheduler) Exist(name string) (exist bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, exist = s.taskMap[name]
	return
}

func (s *Scheduler) GetTask(name string) Task {
	s.lock.Lock()
	defer s.lock.Unlock()
	if taskId, OK := s.taskMap[name]; OK {
		entry := s.cron.Entry(taskId)
		return entry.Job.(Task)
	}
	return nil
}

func (s *Scheduler) GetTaskStatus(name string) *TaskStatus {
	s.lock.Lock()
	defer s.lock.Unlock()
	if taskId, OK := s.taskMap[name]; OK {
		entry := s.cron.Entry(taskId)
		return &TaskStatus{
			entry.Next,
			entry.Prev,
		}
	}
	return nil
}

func (s *Scheduler) RemoveTask(name string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if taskId, OK := s.taskMap[name]; OK {
		s.cron.Remove(taskId)
		delete(s.taskMap, name)
	}
}

func (s *Scheduler) RegisterTask(task Task, cfg *TaskConfig) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, OK := s.taskMap[task.Name()]; OK {
		err = TaskExistsErr
	} else if err = cfg.Check(); err == nil {
		if err = task.Init(cfg.Params); err == nil {
			var taskId cron.EntryID
			if cfg.Cron != "" {
				taskId, err = s.cron.AddJob(cfg.Cron, task)
			} else {
				taskId = s.cron.Schedule(cron.Every(time.Second*time.Duration(cfg.Interval)), task)
			}
			if err == nil {
				s.taskMap[task.Name()] = taskId
			}
		}
	}
	return
}

var TaskExistsErr = errors.New("task already exists")

package scheduler

import (
	"fmt"
	"time"
)

type Task interface {
	Name() string
	Run()
	Init(map[string]interface{}) error
}

type TaskStatus struct {
	Next time.Time `json:"next"`
	Prev time.Time `json:"prev"`
}

type TaskConfig struct {
	Name     string                 `json:"name,omitempty"`
	Cron     string                 `json:"cron,omitempty"`
	Interval int64                  `json:"interval,omitempty"`
	Params   map[string]interface{} `json:"params,omitempty"`
}

func (cfg *TaskConfig) Check() (err error) {
	switch {
	case cfg.Name == "":
		err = fmt.Errorf("task name not found")
	case cfg.Cron == "" && cfg.Interval == 0:
		err = fmt.Errorf("task[%s]execute parameter cron or interval is not specified", cfg.Name)
	case cfg.Cron != "" && cfg.Interval > 0:
		err = fmt.Errorf("task[%s]can not specify both cron and interval", cfg.Name)
	}
	return
}

func (cfg *TaskConfig)AddParam(key string,value interface{}){
	if cfg.Params==nil{
		cfg.Params=make(map[string]interface{})
	}
	cfg.Params[key]=value
}

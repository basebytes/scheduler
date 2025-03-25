package scheduler

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/basebytes/types"
)

func TestTask(t *testing.T) {
	f, err := os.OpenFile("op.log", os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	ops := make(chan *Operate, 100)
	defer close(ops)
	go func() {
		for op := range ops {
			f.WriteString(op.String())
			//c, _ := json.Marshal(op)
			//f.Write(c)
			f.Write([]byte{'\r', '\n'})
		}
	}()
	var wg sync.WaitGroup
	wg.Add(1)
	WithOpLogger(func(op *Operate) {
		ops <- op
	})
	c1 := &TaskConfig{
		Code: "task1",
		Plans: []*PlanConfig{
			{Interval: &types.Duration{Duration: 10 * time.Second}},
		},
	}
	stopAt := time.Now().Add(30 * time.Second)
	c2 := &TaskConfig{
		Code:          "task2",
		MaxDelay:      &types.Duration{Duration: 5 * time.Second},
		MaxWaitingJob: 20,
		Plans: []*PlanConfig{
			{Cron: "0/10 * * * * ?"},
			{Cron: "19 * * * * ?", StopAt: &types.Time{Time: stopAt}},
		},
	}
	if err = RegisterTask(&task1{}, c1); err == nil {
		err = RegisterTask(&task2{}, c2)
	}
	if err != nil {
		panic(err)
	}
	Start()
	end := time.Now().Add(20 * time.Minute)
	log.Printf("scheduler will stop at %s", end.Format(time.DateTime))
	ctx, cancel := context.WithDeadline(context.Background(), end)
	defer cancel()

	go func() {
		wg.Add(1)
		ticker := time.NewTicker(7 * time.Second)
		_ctx, _cancel := context.WithCancel(ctx)
		defer _cancel()
		for {
			select {
			case <-ticker.C:
				jobId, _ := ExecuteTask(c2.Code, nil, &types.Duration{Duration: -1})
				go func() {
					time.Sleep(2 * time.Second)
					_ = CancelJob(c2.Code, PlanManual, jobId)
				}()
			case <-_ctx.Done():
				wg.Done()
				ticker.Stop()
				return
			}
		}
	}()

	go func() {
		wg.Add(1)
		ticker := time.NewTicker(9 * time.Second)
		_ctx, _cancel := context.WithCancel(ctx)
		defer _cancel()
		for {
			select {
			case <-ticker.C:
				_, _ = ExecuteTask(c2.Code, nil, &types.Duration{})
			case <-_ctx.Done():
				wg.Done()
				ticker.Stop()
				return
			}
		}
	}()

	go func() {
		wg.Add(1)
		ticker := time.NewTicker(13 * time.Second)
		_ctx, _cancel := context.WithCancel(ctx)
		defer _cancel()
		for {
			select {
			case <-ticker.C:
				if status := JobStatus(c1.Code); status != nil {
					c, _ := json.Marshal(status)
					log.Printf("%s job status:%s", c1.Code, string(c))
				}
				if status := JobStatus(c2.Code); status != nil {
					c, _ := json.Marshal(status)
					log.Printf("%s job status:%s", c2.Code, string(c))
				}
				if plans := TaskPlanStatus(c1.Code); plans != nil {
					key := c1.Plans[0].Key()
					if status, ok := plans[key]; ok {
						var e error
						if status == Pause {
							e = ResumePlan(c1.Code, key)
						} else {
							e = PausePlan(c1.Code, key)
						}
						if e != nil {
							log.Println(e)
						}
					}
				}
			case <-_ctx.Done():
				wg.Done()
				ticker.Stop()
				return
			}
		}
	}()

	go func() {
		wg.Add(1)
		ticker := time.NewTicker(11 * time.Second)
		_ctx, _cancel := context.WithCancel(ctx)
		planCfg := &PlanConfig{Cron: "0/7 * * * * ?"}
		key := planCfg.Key()
		defer func() {
			wg.Done()
			ticker.Stop()
			_cancel()
		}()
		for {
			select {
			case <-ticker.C:
				if ExistPlan(c1.Code, key) {
					if e := CancelPlan(c1.Code, key); e != nil {
						log.Printf("cancel plan %s from %s err:%s", key, c1.Code, e)
					}
				} else if e := AddPlan(c1.Code, planCfg); e != nil {
					log.Printf("add plan %s to %s err:%s", key, c1.Code, e)
				}
			case <-_ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Add(1)
		ticker := time.NewTicker(11 * time.Second)
		_ctx, _cancel := context.WithCancel(ctx)
		pause := true
		defer func() {
			wg.Done()
			ticker.Stop()
			_cancel()
		}()
		for {
			select {
			case <-ticker.C:
				if pause {
					PauseTask(c2.Code)
					pause = false
				} else {
					ResumeTask(c2.Code)
				}
			case <-_ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Add(1)
		ticker := time.NewTicker(17 * time.Second)
		_ctx, _cancel := context.WithCancel(ctx)
		t3 := &task3{}
		c3 := &TaskConfig{
			Code: "task3",
			Plans: []*PlanConfig{
				{Interval: &types.Duration{Duration: 12 * time.Second}},
			},
		}
		defer func() {
			wg.Done()
			ticker.Stop()
			_cancel()
		}()
		for {
			select {
			case <-ticker.C:
				if ExistTask(c3.Code) {
					c := CancelTask(c3.Code)
					<-c.Done()
					log.Printf("cancel task %s", c3.Code)
				} else if e := RegisterTask(t3, c3); e != nil {
					log.Printf("add task %s err:%s", c3.Code, e)
				}
			case <-_ctx.Done():
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				_ctx := Stop()
				<-_ctx.Done()
				wg.Done()
				return
			}
		}
	}()

	wg.Wait()
	for len(ops) > 0 {
		time.Sleep(time.Second)
	}
	log.Println("finished")

}

type task1 struct {
}

func (t *task1) Code() string {
	return "task1"
}

func (t *task1) Run(now time.Time) {
	time.Sleep(5 * time.Second)
}

func (t *task1) Init(_ map[string]any) error {
	return nil
}

type task2 struct {
}

func (t *task2) Code() string {
	return "task2"
}
func (t *task2) Run(now time.Time) {
	time.Sleep(10 * time.Second)
}

func (t *task2) Init(_ map[string]any) error {
	return nil
}

type task3 struct {
}

func (t *task3) Code() string {
	return "task3"
}
func (t *task3) Run(now time.Time) {
	time.Sleep(7 * time.Second)
}

func (t *task3) Init(_ map[string]any) error {
	return nil
}

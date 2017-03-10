package global_scheduler

import (
	".."
	"sync"
	"time"
)

var instance *timing_wheel.TimingWheel
var timerChan chan TimeoutMsg

type ProcessRef struct {
	Dead bool
	Lock sync.RWMutex
}

func (this *ProcessRef) markDead() {
	this.Lock.Lock()
	this.Dead = true
	this.Lock.Unlock()
}

type TimeoutMsg struct {
	*Scheduler
	Action func()
	Delay  int64
	to     *timing_wheel.Timeout
}

func init() {
	instance = timing_wheel.New(10)
	timerChan = make(chan TimeoutMsg, 1024)
	buf := make([]TimeoutMsg, 0, 1024)
	mainTask := instance.NewInterval(func(intervalTask *timing_wheel.IntervalTask) {
		newBufLen := 0
		for _, msg := range buf {
			if msg.to.Canceled() {
				continue
			}
			msg.proc.Lock.RLock()
			if (!msg.proc.Dead) {
				select {
				case msg.RecvChan <- msg.Action:
				default:
					buf[newBufLen] = msg
					newBufLen ++
				}
			}
			msg.proc.Lock.RUnlock()
		}
		buf = buf[:newBufLen]
	}, 10)
	go func() {
		timer := time.NewTimer(0)
		for !mainTask.Stopped() {
			t1 := time.Now().UnixNano()
			break_point:for {
				select {
				case  msg := <-timerChan:
					ret, _ := instance.NewTimeout(func(to *timing_wheel.Timeout) {
						// TODO: performance
						msg.proc.Lock.RLock()
						if (!msg.proc.Dead) {
							select {
							case msg.RecvChan <- msg.Action:
							default:
								msg.to = to
								buf = append(buf, msg)
							}
						}
						msg.proc.Lock.RUnlock()
					}, msg.Delay)
					msg.retChan <- ret
				case <- timer.C:
					instance.Update()
					t2 := time.Now().UnixNano()
					dt := t2 - t1
					spt := 10 * int64(time.Millisecond) - dt
					if spt < 0 {
						spt = 0
					}
					timer.Reset(time.Duration(spt))
					break break_point
				}
			}
		}
	}()
}

type Scheduler struct {
	RecvChan chan func()
	retChan  chan timing_wheel.TimeoutWrapper
	proc     ProcessRef
}

func (this *Scheduler) Stop() {
	this.proc.markDead()
	close(this.retChan)
	close(this.RecvChan)
}

func NewScheduler() *Scheduler {
	ret := &Scheduler{}
	ret.RecvChan = make(chan func())
	ret.retChan = make(chan timing_wheel.TimeoutWrapper)
	return ret
}

func (this *Scheduler) NewTimeout(action func(), delayMs int64) timing_wheel.TimeoutWrapper {
	timerChan <- TimeoutMsg{Scheduler:this, Action:action, Delay:delayMs}
	ret := <-this.retChan
	return ret
}

func (this *Scheduler) NewInterval(action func(*timing_wheel.IntervalTask), intervalMs int64) *timing_wheel.IntervalTask {
	ret := timing_wheel.CreateIntervalTask()
	this.newInterval(ret, action, intervalMs)
	return ret
}

func (this *Scheduler) newInterval(intervalTask *timing_wheel.IntervalTask, action func(*timing_wheel.IntervalTask), interval int64) {
	wrapper := func() {
		if (!intervalTask.Stopped()) {
			this.newInterval(intervalTask, action, interval)
			action(intervalTask)
		}
	}
	this.NewTimeout(wrapper, interval)
}

func (this *Scheduler) StartCoroutine(steps ...func() interface{}) *timing_wheel.Coroutine {
	ret := timing_wheel.CreateCoroutine()
	this.doCoroutine(ret, 0, steps)
	return ret
}

func (this *Scheduler) doCoroutine(crt *timing_wheel.Coroutine, i int, steps []func() interface{}) {
	if (crt.Stopped()) {
		return
	}
	if i >= len(steps) {
		return
	}
	result := steps[i]()
	if result == nil {
		wrapper := func() {
			this.doCoroutine(crt, i + 1, steps)
		}
		this.NewTimeout(wrapper, 0)
		return
	}

	switch result.(type) {
	case timing_wheel.Loop:
		wt := result.(timing_wheel.Loop).Wait
		wrapper := func() {
			this.doCoroutine(crt, i, steps)
		}
		this.NewTimeout(wrapper, wt)
	case timing_wheel.WaitForMilliSec:
		wt := result.(timing_wheel.WaitForMilliSec).Value
		wrapper := func() {
			this.doCoroutine(crt, i + 1, steps)
		}
		this.NewTimeout(wrapper, wt)
	case timing_wheel.YieldBreak:
		return
	}
}
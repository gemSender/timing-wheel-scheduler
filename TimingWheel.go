package timing_wheel

import (
	"errors"
	"time"
	//"runtime/debug"
	"sync/atomic"
)
/*
type Scheduler interface {
	ScheduleAfterDelay(action func(), delayMs int64) (int, error)
	RemoveTask(taskId int) bool
	TrySchedule() int
}
*/

const (
	Cap1 = 512
	Cap2 = 256
	Cap3 = 128
	Cap4 = 64
	poolSize = 4096 * 4
)

var caps [4]int = [4]int{Cap1, Cap2, Cap3, Cap4}

func GetTimeStampMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond);
}

type Coroutine struct {
	stopped int32
}

func CreateCoroutine() *Coroutine {
	return &Coroutine{}
}

func (this *Coroutine) Stop() {
	atomic.StoreInt32(&this.stopped, 1)
}

func (this *Coroutine) Stopped() bool {
	return atomic.LoadInt32(&this.stopped) == 1
}

type WaitForMilliSec struct {
	Value int64
}

type Loop struct {
	Wait int64
}

type YieldBreak struct {

}

type IntervalTask struct {
	stopped int32
}

func CreateIntervalTask() *IntervalTask {
	return &IntervalTask{}
}

func (this *IntervalTask) Stop() {
	atomic.StoreInt32(&this.stopped, 1)
}

func (this *IntervalTask) Stopped() bool {
	return atomic.LoadInt32(&this.stopped) == 1
}

type Timeout struct {
	id       int64
	canceled int32
	done     int32
}

type TimeoutWrapper struct {
	*Timeout
	Id int64
}

func (t *Timeout) reset(id int64) {
	atomic.StoreInt64(&t.id, id)
	atomic.StoreInt32(&t.canceled, 0)
	atomic.StoreInt32(&t.done, 0)
}

func (t TimeoutWrapper) Cancel() {
	if t.id == atomic.LoadInt64(&t.Timeout.id) {
		atomic.StoreInt32(&t.Timeout.canceled, 1)
	}
}

func (t *Timeout)  Cancel() {
	atomic.StoreInt32(&t.canceled, 1)
}

func (t *Timeout) Canceled() bool {
	return atomic.LoadInt32(&t.canceled) == 1
}

func (t *Timeout) IsDone() bool {
	return atomic.LoadInt32(&t.done) == 1
}

type timeoutMsg struct {
	DelayMs int64
	Action  func()
}

type intervalMsg struct {
	DelayMs int64
	Action  func()
}

type taskObj struct {
	timeout  Timeout
	execTime int64
	action   func(*Timeout)
	next     *taskObj
}

type timeSlot struct {
	execTime int64
	tasks    *taskObj
}

func arrangeTask(que *CapFixedQueue, task *taskObj, timeUnit int64) bool {
	headSlot := que.getHeadElem()
	headSlotTime := headSlot.execTime
	timeDiff := task.execTime - headSlotTime
	if timeDiff < 0 {
		timeDiff = 0
	}
	slotIndex := int(timeDiff / timeUnit)
	if slotIndex < que.count() {
		slot := que.getNth(slotIndex)
		task.next = slot.tasks
		slot.tasks = task
		que.setNth(slotIndex, slot)
		return true
	}
	return false
}

func (this *timeSlot) reset(execTime int64) {
	this.execTime = execTime
}

type TimingWheel struct {
	slotQue     [4]*CapFixedQueue // cap 1024 512 256 128
	timeUnits   [4]int64
	stopped     bool
	taskPool    *taskObj
	timeoutPool *Timeout
	nextTaskId  int64
}

func genSlotQue(now int64, timeUnit int64, len int) *CapFixedQueue {
	ret := newCapFixedQueue(len)
	for i := 0; i < len; i++ {
		ret.enqueue(&timeSlot{execTime: now + int64(i) * timeUnit})
	}
	return ret
}


// 创建一个定时任务调度器, 最小时间间隔以毫秒为单位
func New(timeUnitMs int64) *TimingWheel {
	now := GetTimeStampMs()
	ret := &TimingWheel{
		slotQue : [4]*CapFixedQueue{genSlotQue(now, timeUnitMs, Cap1), nil, nil, nil},
		timeUnits:[4]int64{timeUnitMs, timeUnitMs * Cap1, timeUnitMs * Cap1 * Cap2, timeUnitMs * Cap1 * Cap2 * Cap3},
	}
	bulk := make([]taskObj, poolSize)
	for i := 0; i < poolSize - 1; i++ {
		bulk[i].next = &bulk[i + 1]
	}
	ret.taskPool = &bulk[0]
	return ret
}

func (this *TimingWheel) Stop() {
	this.stopped = true
}

func (this *TimingWheel) Stopped() bool {
	return this.stopped
}

func (this *TimingWheel) getTaskObj() *taskObj {
	ret := this.taskPool
	if ret == nil {
		ret = &taskObj{
		}
	}
	this.taskPool = ret.next
	ret.next = nil
	return ret
}

func (this *TimingWheel) pushTaskBack(t *taskObj) {
	t.next = this.taskPool
	t.action = nil
	t.timeout.reset(-1)
	this.taskPool = t
}

// 创建定时任务, 延迟时间以毫秒为单位
func (this *TimingWheel) NewTimeout(action func(to *Timeout), delayMs int64) (TimeoutWrapper, error) {
	if delayMs < 0 {
		return TimeoutWrapper{Timeout: nil}, errors.New("delay time must not be less than 0 ms")
	}
	now := GetTimeStampMs()
	execTime := now + delayMs
	slotHeadTime := this.slotQue[0].getHeadElem().execTime
	task := this.getTaskObj()
	task.execTime = execTime
	task.action = action
	taskId := this.nextTaskId
	task.timeout.reset(taskId)
	for i := 0; i < 4; i ++ {
		que := this.slotQue[i];
		if que == nil {
			que = genSlotQue(slotHeadTime, this.timeUnits[i], caps[i])
			this.slotQue[i] = que
		}
		if arrangeTask(que, task, this.timeUnits[i]) {
			this.nextTaskId ++
			return TimeoutWrapper{
				Timeout: &task.timeout,
				Id: taskId,
			}, nil
		}
	}
	this.pushTaskBack(task)
	return TimeoutWrapper{Timeout: nil}, errors.New("delay time too large")
}

// 创建间隔任务, 延迟时间以毫秒为单位
func (this *TimingWheel) NewInterval(action func(*IntervalTask), intervalMs int64) *IntervalTask {
	ret := CreateIntervalTask()
	this.newInterval(ret, action, intervalMs)
	return ret
}

func (this *TimingWheel) newInterval(intervalTask *IntervalTask, action func(*IntervalTask), interval int64) {
	wrapper := func(to *Timeout) {
		if (!intervalTask.Stopped()) {
			this.newInterval(intervalTask, action, interval)
			action(intervalTask)
		}
	}
	this.NewTimeout(wrapper, interval)
}

// 创建协程
func (this *TimingWheel) StartCoroutine(steps ...func() interface{}) *Coroutine {
	ret := CreateCoroutine()
	this.doCoroutine(ret, 0, steps)
	return ret
}

func (this *TimingWheel) doCoroutine(crt *Coroutine, i int, steps []func() interface{}) {
	if (crt.Stopped()) {
		return
	}
	if i >= len(steps) {
		return
	}
	result := steps[i]()
	if result == nil {
		wrapper := func(to *Timeout) {
			this.doCoroutine(crt, i + 1, steps)
		}
		this.NewTimeout(wrapper, 0)
		return
	}

	switch result.(type) {
	case Loop:
		wt := result.(Loop).Wait
		wrapper := func(to *Timeout) {
			this.doCoroutine(crt, i, steps)
		}
		this.NewTimeout(wrapper, wt)
	case WaitForMilliSec:
		wt := result.(WaitForMilliSec).Value
		wrapper := func(to *Timeout) {
			this.doCoroutine(crt, i + 1, steps)
		}
		this.NewTimeout(wrapper, wt)
	case YieldBreak:
		return
	}
}

func (this *TimingWheel) Update() {
	now := GetTimeStampMs()
	for i := 3; i > 0; i-- {
		slotQue := this.slotQue[i]
		if slotQue == nil {
			continue
		}
		timeUnit := this.timeUnits[i]
		for {
			headSlot := slotQue.getHeadElem()
			execTime := headSlot.execTime
			if execTime < now {
				for headSlot.tasks != nil {
					task := headSlot.tasks
					headSlot.tasks = task.next
					arrangeTask(this.slotQue[i - 1], task, this.timeUnits[i - 1])
				}
				tailTime := slotQue.getTailElem().execTime
				slotQue.dequeue()
				headSlot.reset(tailTime + timeUnit)
				slotQue.enqueue(headSlot)
			} else {
				break;
			}
		}
	}
	slotQue := this.slotQue[0]
	timeUnit := this.timeUnits[0]
	for {
		headSlot := slotQue.getHeadElem()
		execTime := headSlot.execTime
		if execTime < now {
			for headSlot.tasks != nil {
				task := headSlot.tasks
				headSlot.tasks = task.next
				// TODO: unsafe
				if (!task.timeout.Canceled()) && (!task.timeout.IsDone()) && (task.action != nil) {
					task.action(&task.timeout)
					atomic.StoreInt32(&task.timeout.done, 1)
				}
				this.pushTaskBack(task)
			}
			tailTime := slotQue.getTailElem().execTime
			slotQue.dequeue()
			headSlot.reset(tailTime + timeUnit)
			slotQue.enqueue(headSlot)
		} else {
			break
		}
	}
}

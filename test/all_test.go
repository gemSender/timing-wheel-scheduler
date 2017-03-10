package test

import (
	"testing"
	"log"
	"time"
	"../global_scheduler"
	".."
)

func TestTimeout(t *testing.T) {
	log.Println("----------------------------TestTimeout-------------------------------")
	dt := int64(10)
	scheduler := timing_wheel.New(dt)
	array := make([]int64, 0, 64)
	go func() {
		appendArray := func(timeMs int64) {
			scheduler.NewTimeout(func(t *timing_wheel.Timeout) {
				log.Println("append", timeMs, "to array")
				array = append(array, timeMs)
			}, timeMs)
		}
		appendArray(100)
		appendArray(200)
		appendArray(300)
		appendArray(20)
		appendArray(60)
		appendArray(10)
		appendArray(1000)
		for {
			scheduler.Update()
			time.Sleep(time.Millisecond * time.Duration(dt))
		}
	}()
	time.Sleep(time.Second * 2)
	for i, v := range [...]int64{10, 20, 60, 100, 200, 300, 1000} {
		if v != array[i] {
			t.Error("not in correct sort")
		}
	}
}

func TestInverval(t *testing.T)  {
	log.Println("----------------------------TestInverval-------------------------------")
	dt := int64(10)
	scheduler := timing_wheel.New(dt)
	array := make([]int64, 0, 64)
	go func() {
		counter := int64(0)
		appendArray := func(value int64) {
			log.Println("append", value, "to array")
			array = append(array, value)
		}
		scheduler.NewInterval(func(task *timing_wheel.IntervalTask) {
			for counter >= 10 {
				task.Stop()
				return
			}
			appendArray(counter)
			counter ++
		}, 20)
		for {
			scheduler.Update()
			time.Sleep(time.Millisecond * time.Duration(dt))
		}
	}()
	time.Sleep(time.Second * 2)
	for i, v := range [...]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9} {
		if v != array[i] {
			t.Error("not in correct sort")
		}
	}
}

func TestCoroutine(t *testing.T)  {
	log.Println("----------------------------TestCoroutine-------------------------------")
	dt := int64(10)
	scheduler := timing_wheel.New(dt)
	array := make([]int64, 0, 64)
	go func() {
		counter := 0
		appendArray := func(value int64) {
			log.Println("append", value, "to array")
			array = append(array, value)
		}
		scheduler.StartCoroutine(
			func() interface{} {
				appendArray(timing_wheel.GetTimeStampMs())
				return timing_wheel.WaitForMilliSec{500}
			},
			func() interface{} {
				if counter >= 10 {
					return timing_wheel.YieldBreak{}
				}
				appendArray(timing_wheel.GetTimeStampMs())
				counter ++
				return timing_wheel.Loop{
					100,
				}
			},
		)
		for {
			scheduler.Update()
			time.Sleep(time.Millisecond * time.Duration(dt))
		}
	}()
	time.Sleep(time.Second * 2)
	for i, v := range array {
		if i == 0 {
			continue
		}
		deltaMs := v - array[i - 1]
		if i == 1 {
			if deltaMs - 500 > 10 {
				t.Error("time not accurately")
			}
		} else {
			if deltaMs - 100 > 10 {
				t.Error("time not accurately")
			}
		}
	}
}

func TestGlobalScheduler(t *testing.T)  {
	log.Println("----------------------------TestGlobalScheduler-------------------------------")
	scheduler := global_scheduler.NewScheduler()
	array := make([]int64, 0, 64)
	appendArray := func(value int64) {
		log.Println("append", value, "to array")
		array = append(array, value)
	}
	go func() {
		counter := 0
		scheduler.NewInterval(func(task *timing_wheel.IntervalTask) {
			if counter >= 10 {
				task.Stop()
				return
			}
			appendArray(timing_wheel.GetTimeStampMs())
		}, 100)
		for {
			action := <- scheduler.RecvChan
			action()
		}
	}()
	time.Sleep(time.Second * 2)
	scheduler.Stop()
	for i, v := range array {
		if i == 0 {
			continue
		}
		deltaMs := v - array[i - 1]
		if deltaMs - 100 > 10 {
			t.Error("time not accurately")
		}
	}
}
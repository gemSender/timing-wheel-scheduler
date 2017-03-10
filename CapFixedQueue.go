package timing_wheel

import "errors"

type CapFixedQueue struct{
	slice   []*timeSlot
	heapPos int
	m_count int
}

func newCapFixedQueue(cap int) *CapFixedQueue {
	return &CapFixedQueue{slice:make([]*timeSlot, cap), m_count:0, heapPos:0}
}

func (this *CapFixedQueue) count() int{
	return this.m_count
}

func (this *CapFixedQueue) len() int{
	return len(this.slice)
}

func (this *CapFixedQueue) enqueue(elem *timeSlot){
	cap := len(this.slice)
	if this.m_count < cap{
		this.slice[(this.heapPos + this.m_count) % cap] = elem
		this.m_count ++
	}else{
		this.slice[this.heapPos] = elem
		this.heapPos = (this.heapPos + 1) % cap
	}
}

func (this *CapFixedQueue) dequeue() *timeSlot {
	cap := len(this.slice)
	if this.m_count > 0{
		ret := this.slice[this.heapPos];
		this.slice[this.heapPos] = nil
		this.heapPos = (this.heapPos + 1) % cap
		this.m_count --
		return ret
	}
	return nil
}

func (this *CapFixedQueue) getHeadElem() *timeSlot{
	if this.m_count > 0{
		return this.slice[this.heapPos]
	}
	return nil
}

func (this *CapFixedQueue) getTailElem() *timeSlot{
	if this.m_count > 0{
		return this.slice[(this.heapPos + this.m_count - 1) % len(this.slice)]
	}
	return nil
}

func (this *CapFixedQueue) getNth(index int) *timeSlot {
	if this.m_count > index{
		return this.slice[(this.heapPos + index) % len(this.slice)]
	}
	panic(errors.New("index out of range"))
}

func (this *CapFixedQueue) setNth(index int, value *timeSlot) {
	if this.m_count > index{
		this.slice[(this.heapPos + index) % len(this.slice)] = value
		return
	}
	panic(errors.New("index out of range"))
}


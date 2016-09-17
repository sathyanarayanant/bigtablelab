package btutil

import (
	"sync"
	"time"
)

type Counter struct {
	lock           sync.Mutex
	BucketDuration time.Duration
	bucketStartTime   time.Time
	count          int
}

func NewCounter() *Counter {
	c := Counter{BucketDuration: time.Second * 30, bucketStartTime: time.Now()}

	go c.periodicallyClearOld()
	return &c
}

func (c *Counter) Mark(n int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.count += n
}

func (c *Counter) RatePerSec() float64 {
	c.lock.Lock()
	defer c.lock.Unlock()

	seconds := time.Now().Sub(c.bucketStartTime).Seconds()
	if seconds == 0 {
		return 0
	}
	return float64(c.count) / seconds
}

func (c *Counter) periodicallyClearOld() {
	for {
		now := time.Now()
		c.lock.Lock()
		if c.bucketStartTime.Add(c.BucketDuration).Before(now) {
			c.count = 0
			c.bucketStartTime = time.Now()
		}
		c.lock.Unlock()

		time.Sleep(time.Millisecond * 100)
	}
}

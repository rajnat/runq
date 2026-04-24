package api

import (
	"sync"
	"time"
)

type rateLimiter struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	buckets map[string]*rateBucket
}

type rateBucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(ratePerSecond, burst int) *rateLimiter {
	if ratePerSecond <= 0 || burst <= 0 {
		return nil
	}
	return &rateLimiter{
		rate:    float64(ratePerSecond),
		burst:   float64(burst),
		buckets: make(map[string]*rateBucket),
	}
}

func (l *rateLimiter) Allow(key string, now time.Time) bool {
	if l == nil || key == "" {
		return true
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	bucket, ok := l.buckets[key]
	if !ok {
		l.buckets[key] = &rateBucket{tokens: l.burst - 1, last: now}
		return true
	}

	elapsed := now.Sub(bucket.last).Seconds()
	bucket.tokens += elapsed * l.rate
	if bucket.tokens > l.burst {
		bucket.tokens = l.burst
	}
	bucket.last = now
	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens--
	return true
}

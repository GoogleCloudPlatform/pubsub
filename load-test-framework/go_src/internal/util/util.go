package util

import (
	"runtime"
	"time"
)

func CurrentTimeMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func ScaledNumWorkers(scaleFactor int) int {
	workers := scaleFactor * runtime.NumCPU()
	if workers < 1 {
		workers = 1
	}
	return workers
}

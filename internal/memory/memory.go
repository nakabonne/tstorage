package memory

import (
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

var (
	// TODO: Make it changeable by user.
	//Allowed percent of system memory VictoriaMetrics caches may occupy.
	//"Too low value may increase cache miss rate, which usually results in higher CPU and disk IO usage. "+
	//"Too high value may evict too much data from OS page cache, which will result in higher disk IO usage")
	allowedPercent = 60.0
)

var (
	allowedMemory   int
	remainingMemory int
)

var once sync.Once

func initOnce() {
	mem := sysTotalMemory()
	if allowedPercent < 1 || allowedPercent > 200 {
		logger.Panicf("FATAL: -memory.allowedPercent must be in the range [1...200]; got %g", allowedPercent)
	}
	percent := allowedPercent / 100
	allowedMemory = int(float64(mem) * percent)
	remainingMemory = mem - allowedMemory
	logger.Infof("limiting caches to %d bytes, leaving %d bytes to the OS according to -memory.allowedPercent=%g", allowedMemory, remainingMemory, allowedPercent)
}

// Allowed returns the amount of system memory allowed to use by the app.
//
// The function must be called only after flag.Parse is called.
func Allowed() int {
	once.Do(initOnce)
	return allowedMemory
}

// Remaining returns the amount of memory remaining to the OS.
//
// This function must be called only after flag.Parse is called.
func Remaining() int {
	once.Do(initOnce)
	return remainingMemory
}

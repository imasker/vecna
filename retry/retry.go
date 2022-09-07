package retry

import (
	"fmt"
	"time"

	"github.com/imasker/vecna/log"
)

// Closure - a useful closure we can use when there is a problem
// connecting to the broker. It uses Fibonacci sequence to space out retry attempts
var Closure = func() func(chan int) {
	retryIn := 0
	fibonacci := Fibonacci()
	return func(stopChan chan int) {
		if retryIn > 0 {
			durationString := fmt.Sprintf("%ds", retryIn)
			duration, _ := time.ParseDuration(durationString)

			log.Logger.Warn("Retrying in %d seconds", retryIn)

			select {
			case <-stopChan:
				break
			case <-time.After(duration):
				break
			}
		}
		retryIn = fibonacci()
	}
}

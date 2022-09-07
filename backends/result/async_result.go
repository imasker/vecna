package result

import (
	"errors"
	"reflect"
	"time"

	"github.com/imasker/vecna/backends/iface"
	"github.com/imasker/vecna/tasks"
)

var (
	// ErrBackendNotConfigured ...
	ErrBackendNotConfigured = errors.New("result backend not configured")
	// ErrTimeoutReached ...
	ErrTimeoutReached = errors.New("timeout reached")
)

// AsyncResult represents a task result
type AsyncResult struct {
	Signature *tasks.Signature
	taskState *tasks.TaskState
	backend   iface.Backend
}

// ChordAsyncResult represents a result of a chord
type ChordAsyncResult struct {
	groupAsyncResults []*AsyncResult
	chordAsyncResult  *AsyncResult
	backend           iface.Backend
}

// ChainAsyncResult represents a result of a chain of tasks
type ChainAsyncResult struct {
	asyncResults []*AsyncResult
	backend      iface.Backend
}

// NewAsyncResult creates AsyncResult instance
func NewAsyncResult(signature *tasks.Signature, backend iface.Backend) *AsyncResult {
	return &AsyncResult{
		Signature: signature,
		taskState: new(tasks.TaskState),
		backend:   backend,
	}
}

// NewChordAsyncResult creates ChordAsyncResult instance
func NewChordAsyncResult(groupTasks []*tasks.Signature, chordCallback *tasks.Signature, backend iface.Backend) *ChordAsyncResult {
	asyncResults := make([]*AsyncResult, len(groupTasks))
	for i, task := range groupTasks {
		asyncResults[i] = NewAsyncResult(task, backend)
	}
	return &ChordAsyncResult{
		groupAsyncResults: asyncResults,
		chordAsyncResult:  NewAsyncResult(chordCallback, backend),
		backend:           backend,
	}
}

// NewChainAsyncResult creates ChainAsyncResult instance
func NewChainAsyncResult(tasks []*tasks.Signature, backend iface.Backend) *ChainAsyncResult {
	asyncResults := make([]*AsyncResult, len(tasks))
	for i, task := range tasks {
		asyncResults[i] = NewAsyncResult(task, backend)
	}
	return &ChainAsyncResult{
		asyncResults: asyncResults,
		backend:      backend,
	}
}

// Touch the state and don't wait
func (a *AsyncResult) Touch() ([]reflect.Value, error) {
	if a.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	a.GetState()

	if a.taskState.IsFailure() {
		return nil, errors.New(a.taskState.Error)
	}

	if a.taskState.IsSuccess() {
		return tasks.ReflectTaskResults(a.taskState.Results)
	}

	if a.taskState.IsCanceled() {
		return nil, errors.New("canceled")
	}

	return nil, nil
}

// Get returns task results (synchronous blocking call)
func (a *AsyncResult) Get(sleepDuration time.Duration) ([]reflect.Value, error) {
	for {
		results, err := a.Touch()

		if results == nil && err == nil {
			time.Sleep(sleepDuration)
		} else {
			return results, err
		}
	}
}

// GetWithTimeout returns task results with a timeout (synchronous blocking call)
func (a *AsyncResult) GetWithTimeout(timeoutDuration, sleepDuration time.Duration) ([]reflect.Value, error) {
	timeout := time.NewTimer(timeoutDuration)

	for {
		select {
		case <-timeout.C:
			return nil, ErrTimeoutReached
		default:
			results, err := a.Touch()

			if results == nil && err == nil {
				time.Sleep(sleepDuration)
			} else {
				return results, err
			}
		}
	}
}

// GetState returns latest task state
func (a *AsyncResult) GetState() *tasks.TaskState {
	if a.taskState.IsCompleted() || a.taskState.IsCanceled() {
		return a.taskState
	}

	taskState, err := a.backend.GetState(a.Signature.ID)
	if err == nil {
		a.taskState = taskState
	}

	return a.taskState
}

// Get returns results of a chain of tasks (synchronous block call)
func (c *ChainAsyncResult) Get(sleepDuration time.Duration) ([]reflect.Value, error) {
	if c.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	var (
		results []reflect.Value
		err     error
	)

	for _, asyncResult := range c.asyncResults {
		results, err = asyncResult.Get(sleepDuration)
		if err != nil {
			return nil, err
		}
	}

	return results, err
}

// Get returns result of a chord (synchronous blocking call)
func (c *ChordAsyncResult) Get(sleepDuration time.Duration) ([]reflect.Value, error) {
	if c.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	var err error
	for _, asyncResult := range c.groupAsyncResults {
		_, err = asyncResult.Get(sleepDuration)
		if err != nil {
			return nil, err
		}
	}

	return c.chordAsyncResult.Get(sleepDuration)
}

// GetWithTimeout returns results of a chain of tasks with timeout (synchronous blocking call)
func (c *ChainAsyncResult) GetWithTimeout(timeoutDuration, sleepDuration time.Duration) ([]reflect.Value, error) {
	if c.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	var (
		results []reflect.Value
		err     error
	)

	timeout := time.NewTimer(timeoutDuration)
	ln := len(c.asyncResults)
	lastResult := c.asyncResults[ln-1]

	for {
		select {
		case <-timeout.C:
			return nil, ErrTimeoutReached
		default:
			for _, asyncResult := range c.asyncResults {
				_, err = asyncResult.Touch()
				if err != nil {
					return nil, err
				}
			}

			results, err = lastResult.Touch()
			if err != nil {
				return nil, err
			}
			if results != nil {
				return results, err
			}
			time.Sleep(sleepDuration)
		}
	}
}

// GetWithTimeout returns result of a chord with a timeout (synchronous blocking call)
func (c *ChordAsyncResult) GetWithTimeout(timeoutDuration, sleepDuration time.Duration) ([]reflect.Value, error) {
	if c.backend == nil {
		return nil, ErrBackendNotConfigured
	}

	var (
		results []reflect.Value
		err     error
	)

	timeout := time.NewTimer(timeoutDuration)
	for {
		select {
		case <-timeout.C:
			return nil, ErrTimeoutReached
		default:
			for _, asyncResult := range c.groupAsyncResults {
				//TODO: why use errcur
				_, errCur := asyncResult.Touch()
				if errCur != nil {
					return nil, err
				}
			}

			results, err = c.chordAsyncResult.Touch()
			//TODO: why return nil as error
			if err != nil {
				return nil, nil
			}
			if results != nil {
				return results, err
			}
			time.Sleep(sleepDuration)
		}
	}
}

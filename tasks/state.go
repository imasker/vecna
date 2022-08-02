package tasks

import "time"

const (
	// StatePending - initial state of a task
	StatePending = "PENDING"
	// StateReceived - when task is received by a worker
	StateReceived = "RECEIVED"
	// StateStarted - when the worker starts processing the task
	StateStarted = "STARTED"
	// StateRetry - when failed task has been scheduled for retry
	StateRetry = "RETRY"
	// StateSuccess - when the task is processed successfully
	StateSuccess = "SUCCESS"
	// StateFailure - when processing of the task fails
	StateFailure = "FAILURE"
)

// TaskState represents a state of a task
type TaskState struct {
	TaskID    string
	TaskName  string
	State     string
	Results   []*TaskResult
	Error     string
	CreatedAt time.Time
	TTL       int64
}

// GroupMeta stores useful metadata about tasks within the same group
// E.g. IDs of all tasks which are used in order to check if all tasks
// completed successfully or not and thus whether to trigger chord callback
type GroupMeta struct {
	GroupID        string
	TaskIDs        []string
	ChordTriggered bool
	Lock           bool
	CreatedAt      time.Time
	TTL            int64
}

// NewPendingTaskState ...
func NewPendingTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskID:    signature.ID,
		TaskName:  signature.Name,
		State:     StatePending,
		CreatedAt: time.Now().UTC(),
	}
}

// NewReceivedTaskState ...
func NewReceivedTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskID: signature.ID,
		State:  StateReceived,
	}
}

// NewStartedTaskState ...
func NewStartedTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskID: signature.ID,
		State:  StateStarted,
	}
}

// NewSuccessTaskState ...
func NewSuccessTaskState(signature *Signature, results []*TaskResult) *TaskState {
	return &TaskState{
		TaskID:  signature.ID,
		State:   StateSuccess,
		Results: results,
	}
}

// NewFailureTaskState ...
func NewFailureTaskState(signature *Signature, err string) *TaskState {
	return &TaskState{
		TaskID: signature.ID,
		State:  StateFailure,
		Error:  err,
	}
}

// NewRetryTaskState ...
func NewRetryTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskID: signature.ID,
		State:  StateRetry,
	}
}

// IsCompleted returns true if state is SUCCESS or FAILURE,
// i.e. the task has finished processing and either succeeded or failed.
func (t *TaskState) IsCompleted() bool {
	return t.IsSuccess() || t.IsFailure()
}

// IsSuccess returns true if state is SUCCESS
func (t *TaskState) IsSuccess() bool {
	return t.State == StateSuccess
}

// IsFailure returns true if state is FAILURE
func (t *TaskState) IsFailure() bool {
	return t.State == StateFailure
}

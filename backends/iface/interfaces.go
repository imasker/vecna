package iface

import "vecna/tasks"

// Backend - a common interface for all result backends
type Backend interface {
	// Group related functions
	InitGroup(groupID string, taskIDs []string) error
	GroupCompleted(groupID string, groupTaskCount int) (bool, error)
	GroupTaskStates(groupID string, groupTaskCount int) ([]*tasks.TaskState, error)
	TriggerChord(groupID string) (bool, error)

	// Setting / getting task state
	SetStatePending(signature *tasks.Signature) error
	SetStateReceived(signature *tasks.Signature) error
	SetStateStarted(signature *tasks.Signature) error
	SetStateRetry(signature *tasks.Signature) error
	SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error
	SetStateFailure(signature *tasks.Signature, err string) error
	SetStateCanceled(signature *tasks.Signature) error
	GetState(taskID string) (*tasks.TaskState, error)

	// Purging stored task states and group meta data
	PurgeState(taskID string) error
	PurgeGroupMeta(groupID string) error
}

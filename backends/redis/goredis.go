package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"
	"vecna/backends/iface"
	"vecna/common"
	"vecna/config"
	"vecna/log"
	"vecna/tasks"
	"vecna/utils"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	redsyncgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v8"
)

// Backend represents a Redis result backend with go-redis
type Backend struct {
	common.Backend
	client  redis.UniversalClient
	redsync *redsync.Redsync
}

// New creates Backend instance
func New(cnf *config.Config) (iface.Backend, error) {
	b := &Backend{
		Backend: common.NewBackend(cnf),
	}
	opt, err := utils.ParseRedisURL(cnf.ResultBackend)
	if err != nil {
		return nil, err
	}
	if cnf.Redis != nil {
		opt.MasterName = cnf.Redis.MasterName
	}

	b.client = redis.NewUniversalClient(opt)
	b.redsync = redsync.New(redsyncgoredis.NewPool(b.client))
	return b, nil
}

// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(groupID string, taskIDs []string) error {
	groupMeta := &tasks.GroupMeta{
		GroupID:   groupID,
		TaskIDs:   taskIDs,
		CreatedAt: time.Now().UTC(),
	}

	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	expiration := b.getExpiration()
	err = b.client.Set(context.Background(), groupID, encoded, expiration).Err()
	if err != nil {
		return err
	}

	return nil
}

// GroupCompleted returns true if all tasks in a group finished
func (b *Backend) GroupCompleted(groupID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(groupMeta.TaskIDs...)
	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *Backend) GroupTaskStates(groupID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	return b.getStates(groupMeta.TaskIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never triggered multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *Backend) TriggerChord(groupID string) (bool, error) {
	m := b.redsync.NewMutex("TriggerChordMutex")
	if err := m.Lock(); err != nil {
		return false, err
	}
	defer m.Unlock()

	groupMeta, err := b.getGroupMeta(groupID)
	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// Set flag to true
	groupMeta.ChordTriggered = true

	// Update the group meta
	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}

	expiration := b.getExpiration()
	err = b.client.Set(context.Background(), groupID, encoded, expiration).Err()
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *Backend) mergeNewTaskState(newState *tasks.TaskState) {
	state, err := b.GetState(newState.TaskID)
	if err == nil {
		newState.CreatedAt = state.CreatedAt
		newState.TaskName = state.TaskName
	}
}

// SetStatePending updates task state to PENDING
func (b *Backend) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *Backend) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateStarted updates task state to STARTED
func (b *Backend) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateRetry updates task state to RETRY
func (b *Backend) SetStateRetry(signature *tasks.Signature) error {
	taskState := tasks.NewRetryTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateSuccess updates task state to SUCCESS
func (b *Backend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *Backend) SetStateFailure(signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// SetStateCanceled updates task state to CANCELED
func (b *Backend) SetStateCanceled(signature *tasks.Signature) error {
	taskState := tasks.NewCanceledTaskState(signature)
	b.mergeNewTaskState(taskState)
	return b.updateState(taskState)
}

// GetState returns the latest task state
func (b *Backend) GetState(taskID string) (*tasks.TaskState, error) {
	item, err := b.client.Get(context.Background(), taskID).Bytes()
	if err != nil {
		return nil, err
	}
	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *Backend) PurgeState(taskID string) error {
	err := b.client.Del(context.Background(), taskID).Err()
	if err != nil {
		return err
	}

	return nil
}

// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(groupID string) error {
	err := b.client.Del(context.Background(), groupID).Err()
	if err != nil {
		return err
	}

	return nil
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *Backend) getGroupMeta(groupID string) (*tasks.GroupMeta, error) {
	item, err := b.client.Get(context.Background(), groupID).Bytes()
	if err != nil {
		return nil, err
	}

	groupMeta := new(tasks.GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// getStates returns multiple task states
func (b *Backend) getStates(taskIDs ...string) ([]*tasks.TaskState, error) {
	taskStates := make([]*tasks.TaskState, len(taskIDs))
	results, err := b.client.MGet(context.Background(), taskIDs...).Result()
	if err != nil {
		return taskStates, err
	}
	for i, result := range results {
		if result == nil {
			return taskStates, redis.Nil
		}
		taskState := new(tasks.TaskState)
		decoder := json.NewDecoder(strings.NewReader(result.(string)))
		decoder.UseNumber()
		if err = decoder.Decode(taskState); err != nil {
			log.Logger.Error("%v", err)
			return taskStates, err
		}
		taskStates[i] = taskState
	}

	return taskStates, nil
}

// updateState saves current task state
func (b *Backend) updateState(taskState *tasks.TaskState) error {
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}

	expiration := b.getExpiration()
	err = b.client.Set(context.Background(), taskState.TaskID, encoded, expiration).Err()
	if err != nil {
		return err
	}

	return nil
}

// getExpiration returns expiration for a stored task state
func (b *Backend) getExpiration() time.Duration {
	expiresIn := b.GetConfig().ResultsExpireIn
	if expiresIn == 0 {
		// expire results after 1 hour by default
		expiresIn = config.DefaultResultsExpireIn
	}

	return time.Duration(expiresIn) * time.Second
}

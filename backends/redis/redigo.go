package redis

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"vecna/backends/iface"
	"vecna/common"
	"vecna/config"
	"vecna/log"
	"vecna/tasks"

	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis/redigo"
	"github.com/gomodule/redigo/redis"
)

// BackendRG represents a Redis result backend with redigo
type BackendRG struct {
	common.Backend
	host     string
	password string
	db       int
	pool     *redis.Pool
	// If set, path to a socket file overrides hostname
	socketPath string
	redsync    *redsync.Redsync
	redisOnce  sync.Once
	common.RedisConnector
}

// NewRG creates BackendRG instance
func NewRG(cnf *config.Config, host, password, socketPath string, db int) iface.Backend {
	return &BackendRG{
		Backend:    common.NewBackend(cnf),
		host:       host,
		db:         db,
		password:   password,
		socketPath: socketPath,
	}
}

// InitGroup creates and saves a group meta data object
func (b *BackendRG) InitGroup(groupID string, taskIDs []string) error {
	groupMeta := &tasks.GroupMeta{
		GroupID:   groupID,
		TaskIDs:   taskIDs,
		CreatedAt: time.Now().UTC(),
	}

	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	conn := b.open()
	defer conn.Close()

	expiration := int64(b.getExpiration().Seconds())
	_, err = conn.Do("SET", groupID, encoded, "EX", expiration)
	if err != nil {
		return err
	}

	return nil
}

// GroupCompleted returns true if all tasks in a group finished
func (b *BackendRG) GroupCompleted(groupID string, groupTaskCount int) (bool, error) {
	conn := b.open()
	defer conn.Close()

	groupMeta, err := b.getGroupMeta(conn, groupID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(conn, groupMeta.TaskIDs...)
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
func (b *BackendRG) GroupTaskStates(groupID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	conn := b.open()
	defer conn.Close()

	groupMeta, err := b.getGroupMeta(conn, groupID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	return b.getStates(conn, groupMeta.TaskIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never triggered multiple times. Returns a boolean flag to indicate
// weather the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *BackendRG) TriggerChord(groupID string) (bool, error) {
	conn := b.open()
	defer conn.Close()

	m := b.redsync.NewMutex("TriggerChordMutex")
	if err := m.Lock(); err != nil {
		return false, err
	}
	defer m.Unlock()

	groupMeta, err := b.getGroupMeta(conn, groupID)
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

	expiration := int64(b.getExpiration().Seconds())
	_, err = conn.Do("SET", groupID, encoded, "EX", expiration)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *BackendRG) mergeNewTaskState(conn redis.Conn, newState *tasks.TaskState) {
	state, err := b.getState(conn, newState.TaskID)
	if err == nil {
		newState.CreatedAt = state.CreatedAt
		newState.TaskName = state.TaskName
	}
}

// SetStatePending updates task state to PENDING
func (b *BackendRG) SetStatePending(signature *tasks.Signature) error {
	conn := b.open()
	defer conn.Close()

	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(conn, taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *BackendRG) SetStateReceived(signature *tasks.Signature) error {
	conn := b.open()
	defer conn.Close()

	taskState := tasks.NewReceivedTaskState(signature)
	b.mergeNewTaskState(conn, taskState)
	return b.updateState(conn, taskState)
}

// SetStateStarted updates task state to STARTED
func (b *BackendRG) SetStateStarted(signature *tasks.Signature) error {
	conn := b.open()
	defer conn.Close()

	taskState := tasks.NewStartedTaskState(signature)
	b.mergeNewTaskState(conn, taskState)
	return b.updateState(conn, taskState)
}

// SetStateRetry updates task state to RETRY
func (b *BackendRG) SetStateRetry(signature *tasks.Signature) error {
	conn := b.open()
	defer conn.Close()

	taskState := tasks.NewRetryTaskState(signature)
	b.mergeNewTaskState(conn, taskState)
	return b.updateState(conn, taskState)
}

// SetStateSuccess updates task state to SUCCESS
func (b *BackendRG) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	conn := b.open()
	defer conn.Close()

	taskState := tasks.NewSuccessTaskState(signature, results)
	b.mergeNewTaskState(conn, taskState)
	return b.updateState(conn, taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *BackendRG) SetStateFailure(signature *tasks.Signature, err string) error {
	conn := b.open()
	defer conn.Close()

	taskState := tasks.NewFailureTaskState(signature, err)
	b.mergeNewTaskState(conn, taskState)
	return b.updateState(conn, taskState)
}

// GetState returns the latest task state
func (b *BackendRG) GetState(taskID string) (*tasks.TaskState, error) {
	conn := b.open()
	defer conn.Close()

	return b.getState(conn, taskID)
}

func (b *BackendRG) getState(conn redis.Conn, taskID string) (*tasks.TaskState, error) {
	item, err := redis.Bytes(conn.Do("GET", taskID))
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
func (b *BackendRG) PurgeState(taskID string) error {
	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("DEL", taskID)
	if err != nil {
		return err
	}

	return nil
}

// PurgeGroupMeta deletes stored group meta data
func (b *BackendRG) PurgeGroupMeta(groupID string) error {
	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("DEL", groupID)
	if err != nil {
		return err
	}

	return nil
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *BackendRG) getGroupMeta(conn redis.Conn, groupID string) (*tasks.GroupMeta, error) {
	item, err := redis.Bytes(conn.Do("GET", groupID))
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
func (b *BackendRG) getStates(conn redis.Conn, taskIDs ...string) ([]*tasks.TaskState, error) {
	taskStates := make([]*tasks.TaskState, len(taskIDs))

	// conn.Do requires []interface{}... can't pass []string unfortunately
	taskIDInterfaces := make([]interface{}, len(taskIDs))
	for i, taskID := range taskIDs {
		taskIDInterfaces[i] = taskID
	}

	reply, err := redis.Values(conn.Do("MGET", taskIDInterfaces...))
	if err != nil {
		return taskStates, err
	}

	for i, value := range reply {
		stateBytes, ok := value.([]byte)
		if !ok {
			return taskStates, fmt.Errorf("expected byte array, instead got: %v", value)
		}

		taskState := new(tasks.TaskState)
		decoder := json.NewDecoder(bytes.NewReader(stateBytes))
		decoder.UseNumber()
		if err := decoder.Decode(taskState); err != nil {
			log.Logger.Error("%s", err)
			return taskStates, err
		}

		taskStates[i] = taskState
	}

	return taskStates, nil
}

// updateState saves current task state
func (b *BackendRG) updateState(conn redis.Conn, taskState *tasks.TaskState) error {
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}

	expiration := int64(b.getExpiration().Seconds())
	_, err = conn.Do("SET", taskState.TaskID, encoded, "EX", expiration)
	if err != nil {
		return err
	}

	return nil
}

// getExpiration returns expiration for a stored task state
func (b *BackendRG) getExpiration() time.Duration {
	expiresIn := b.GetConfig().ResultsExpireIn
	if expiresIn == 0 {
		// expire results after 1 hour by default
		expiresIn = config.DefaultResultsExpireIn
	}

	return time.Duration(expiresIn) * time.Second
}

// open returns or creates instance of Redis connection
func (b *BackendRG) open() redis.Conn {
	b.redisOnce.Do(func() {
		b.pool = b.NewPool(b.socketPath, b.host, b.password, b.db, b.GetConfig().Redis, b.GetConfig().TLSConfig)
		b.redsync = redsync.New(redsyncredis.NewPool(b.pool))
	})
	return b.pool.Get()
}

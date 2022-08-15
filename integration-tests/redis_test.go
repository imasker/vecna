package integration_tests

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/stretchr/testify/assert"

	"vecna"
	"vecna/config"
	"vecna/tasks"
)

var redisServer *miniredis.Miniredis
var server *vecna.Server
var worker *vecna.Worker
var setupOnce = &sync.Once{}
var teardownOnce = &sync.Once{}

func mockRedis() *miniredis.Miniredis {
	s, err := miniredis.Run()

	if err != nil {
		panic(err)
	}

	return s
}

func setup() {
	fmt.Println("setup...")
	setupOnce.Do(func() {
		fmt.Println("setup in...")
		redisServer = mockRedis()
		redisUrl := fmt.Sprintf("redis://%s", redisServer.Addr())
		cnf := &config.Config{
			Broker:          redisUrl,
			Lock:            redisUrl,
			ResultBackend:   redisUrl,
			DefaultQueue:    "vecna_tasks",
			ResultsExpireIn: 3600,
			Redis: &config.RedisConfig{
				NormalTasksPollPeriod:  1000,
				DelayedTasksPollPeriod: 500,
			},
		}

		server, _ = vecna.NewServer(cnf)

		registerTestTasks(server)
		registerRedisTestTasks(server)

		worker = server.NewWorker("test_worker", 0)
		go worker.Launch()
	})
}

func teardown() {
	fmt.Println("teardown...")
	teardownOnce.Do(func() {
		fmt.Println("teardown in...")
		worker.Quit()
		redisServer.Close()
	})
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func TestServer_SendTask(t *testing.T) {
	addTask := newAddTask(1, 1)

	asyncResult, err := server.SendTask(addTask)
	if err != nil {
		t.Error(err)
	}

	results, err := asyncResult.Get(5 * time.Millisecond)
	if err != nil {
		t.Error(err)
	}

	if len(results) != 1 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
	}

	if results[0].Interface() != int64(2) {
		t.Errorf("result = %s(%v), want int64(2)", results[0].Type().String(), results[0].Interface())
	}

	sumTask := newSumTask([]int64{1, 2})
	asyncResult, err = server.SendTask(sumTask)
	if err != nil {
		t.Error(err)
	}

	results, err = asyncResult.Get(5 * time.Millisecond)
	if err != nil {
		t.Error(err)
	}

	if len(results) != 1 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
	}

	if results[0].Interface() != int64(3) {
		t.Errorf("result = %s(%v), want int64(3)", results[0].Type().String(), results[0].Interface())
	}
}

func TestServer_SendGroup(t *testing.T) {
	t1, t2, t3 := newAddTask(1, 1), newAddTask(2, 2), newAddTask(5, 6)

	group, _ := tasks.NewGroup(t1, t2, t3)

	asyncResults, err := server.SendGroup(group, 10)
	if err != nil {
		t.Error(err)
	}

	expectedResults := []int64{2, 4, 11}

	actualResults := make([]int64, 3)

	for i, asyncResult := range asyncResults {
		results, err := asyncResult.Get(5 * time.Millisecond)
		if err != nil {
			t.Error(err)
		}

		if len(results) != 1 {
			t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
		}

		intResult, ok := results[0].Interface().(int64)
		if !ok {
			t.Errorf("Could not convert %v to int64", results[0].Interface())
		}
		actualResults[i] = intResult
	}

	sort.Sort(ascendingInt64s(actualResults))

	if !reflect.DeepEqual(expectedResults, actualResults) {
		t.Errorf("expected results = %v, acutal results = %v", expectedResults, actualResults)
	}
}

func TestServer_SendChain(t *testing.T) {
	t1, t2, t3 := newAddTask(2, 2), newAddTask(5, 6), newMultipleTask(4)

	chain, _ := tasks.NewChain(t1, t2, t3)

	chainAsyncResult, err := server.SendChain(chain)
	if err != nil {
		t.Error(err)
	}

	results, err := chainAsyncResult.Get(5 * time.Millisecond)
	if err != nil {
		t.Error(err)
	}

	if len(results) != 1 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
	}

	if results[0].Interface() != int64(60) {
		t.Errorf("result = %s(%v), want int64(60)", results[0].Type().String(), results[0].Interface())
	}
}

func TestServer_SendChord(t *testing.T) {
	t1, t2, t3, t4 := newAddTask(1, 1), newAddTask(2, 2), newAddTask(5, 6), newMultipleTask()

	group, _ := tasks.NewGroup(t1, t2, t3)

	chord, _ := tasks.NewChord(group, t4)

	chordAsyncResult, err := server.SendChord(chord, 10)
	if err != nil {
		t.Error(err)
	}

	results, err := chordAsyncResult.Get(5 * time.Millisecond)
	if err != nil {
		t.Error(err)
	}

	if len(results) != 1 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
	}

	if results[0].Interface() != int64(88) {
		t.Errorf("result = %s(%v), want int64(88)", results[0].Type().String(), results[0].Interface())
	}
}

func TestReturnJustError(t *testing.T) {
	// Fails, returns error as the only value
	task := newErrorTask("Test error", true)
	asyncResult, err := server.SendTask(task)
	if err != nil {
		t.Error(err)
	}

	results, err := asyncResult.Get(5 * time.Millisecond)
	if len(results) != 0 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 0)
	}
	assert.Equal(t, "Test error", err.Error())

	// Successful, returns nil as the only value
	task = newErrorTask("", false)
	asyncResult, err = server.SendTask(task)
	if err != nil {
		t.Error(err)
	}

	results, err = asyncResult.Get(5 * time.Millisecond)
	if len(results) != 0 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 0)
	}
	assert.NoError(t, err)
}

func TestReturnMultipleValues(t *testing.T) {
	// Successful task with multiple return values
	task := newMultipleReturnTask("foo", "bar", false)

	asyncResult, err := server.SendTask(task)
	if err != nil {
		t.Error(err)
	}

	results, err := asyncResult.Get(5 * time.Millisecond)
	if err != nil {
		t.Error(err)
	}

	if len(results) != 2 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 2)
	}

	if results[0].Interface() != "foo" {
		t.Errorf("result = %s(%v), want string(foo)", results[0].Type().String(), results[0].Interface())
	}

	if results[1].Interface() != "bar" {
		t.Errorf("result = %s(%v), want string(bar)", results[1].Type().String(), results[1].Interface())
	}

	// Failed task with multiple return values
	task = newMultipleReturnTask("", "", true)

	asyncResult, err = server.SendTask(task)
	if err != nil {
		t.Error(err)
	}

	results, err = asyncResult.Get(5 * time.Millisecond)
	if len(results) != 0 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 0)
	}
	assert.Error(t, err)
}

func TestPanic(t *testing.T) {
	task := &tasks.Signature{Name: "panic"}
	asyncResult, err := server.SendTask(task)
	if err != nil {
		t.Error(err)
	}

	results, err := asyncResult.Get(5 * time.Millisecond)
	if len(results) != 0 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 0)
	}
	assert.Equal(t, "oops", err.Error())
}

func TestDelay(t *testing.T) {
	now := time.Now().UTC()
	eta := now.Add(100 * time.Millisecond)
	task := newDelayTask(eta)
	asyncResult, err := server.SendTask(task)
	if err != nil {
		t.Error(err)
	}

	results, err := asyncResult.Get(5 * time.Millisecond)
	if err != nil {
		t.Error(err)
	}

	if len(results) != 1 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
	}

	tm, ok := results[0].Interface().(int64)
	if !ok {
		t.Errorf("Could not type assert = %s(%v) to int64", results[0].Type().String(), results[0].Interface())
	}

	if tm < eta.UnixNano() {
		t.Errorf("result = %s(%v), want >= int64(%d)", results[0].Type().String(), results[0].Interface(), eta.UnixNano())
	}
}

func TestDelay2(t *testing.T) {
	now := time.Now().UTC()

	eta := now.Add(100 * time.Second)
	task := newDelayTask(eta)
	_, err := server.SendTask(task)
	if err != nil {
		t.Error(err)
	}

	eta = now.Add(100 * time.Millisecond)
	task = newDelayTask(eta)
	asyncResult, err := server.SendTask(task)
	if err != nil {
		t.Error(err)
	}

	results, err := asyncResult.Get(5 * time.Millisecond)
	if err != nil {
		t.Error(err)
	}

	if len(results) != 1 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
	}

	tm, ok := results[0].Interface().(int64)
	if !ok {
		t.Errorf("Could not type assert = %s(%v) to int64", results[0].Type().String(), results[0].Interface())
	}

	if tm < eta.UnixNano() {
		t.Errorf("result = %s(%v), want >= int64(%d)", results[0].Type().String(), results[0].Interface(), eta.UnixNano())
	}
}

func TestPeriodic(t *testing.T) {
	key := "test_periodic"
	task := newPeriodicTask(key, 1)
	asyncResult, err := server.SendPeriodicTask(Spec, task)
	assert.NoError(t, err)
	results, err := asyncResult.Get(100 * time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, results[0].Interface().(int))
	value, err := redisServer.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, "1", value)
	//time.Sleep(70 * time.Second)
	//value, err = redisServer.Get(key)
	//assert.NoError(t, err)
	//assert.Equal(t, "2", value)
}

func TestPeriodicChain(t *testing.T) {
	key := "test_periodic_chain"
	task1, task2, task3 := newPeriodicTask(key, 1), newPeriodicTask(key, 2), newPeriodicTask(key, 3)
	chain, err := tasks.NewChain(task1, task2, task3)
	assert.NoError(t, err)
	asyncResult, err := server.SendPeriodicChain(Spec, chain)
	assert.NoError(t, err)
	results, err := asyncResult.Get(100 * time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 6, results[0].Interface().(int))
	value, err := redisServer.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, "6", value)
}

func TestPeriodicGroup(t *testing.T) {
	key := "test_periodic_group"
	task1, task2, task3 := newPeriodicTask(key, 1), newPeriodicTask(key, 2), newPeriodicTask(key, 3)
	group, err := tasks.NewGroup(task1, task2, task3)
	assert.NoError(t, err)
	asyncResults, err := server.SendPeriodicGroup(Spec, group, 10)
	assert.NoError(t, err)
	for _, asyncResult := range asyncResults {
		results, err := asyncResult.Get(100 * time.Millisecond)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(results))
	}
	value, err := redisServer.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, "6", value)
}

func TestPeriodicChord(t *testing.T) {
	key := "test_periodic_chord"
	task1, task2, task3 := newPeriodicTask(key, 1), newPeriodicTask(key, 2), newPeriodicTask(key, 3)
	task4 := newPeriodicTask(key, 10)
	group, err := tasks.NewGroup(task1, task2, task3)
	assert.NoError(t, err)
	chord, err := tasks.NewChord(group, task4)
	assert.NoError(t, err)
	asyncResult, err := server.SendPeriodicChord(Spec, chord, 10)
	assert.NoError(t, err)
	results, err := asyncResult.Get(100 * time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 16, results[0].Interface().(int))
	value, err := redisServer.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, "16", value)
}

func TestCancelDelayedTask(t *testing.T) {
	key := "test_cancel_delayed_task"
	err := redisServer.Set(key, "0")
	assert.NoError(t, err)
	task := newDelayedTask(key, 1)
	asyncResult, err := server.SendTask(task)
	assert.NoError(t, err)
	err = server.CancelDelayedTask(task)
	assert.NoError(t, err)
	assert.True(t, asyncResult.GetState().IsCanceled())
	_, err = asyncResult.Get(100 * time.Millisecond)
	assert.Error(t, err)
	assert.Equal(t, "canceled", err.Error())
	time.Sleep(3 * time.Second)
	value, err := redisServer.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, "0", value)
}

func TestCancelPeriodicTask(t *testing.T) {
	key := "test_cancel_periodic_task"
	err := redisServer.Set(key, "0")
	assert.NoError(t, err)
	task := newPeriodicTask(key, 1)
	asyncResult, err := server.SendPeriodicTask(Spec, task)
	assert.NoError(t, err)
	err = server.CancelPeriodicTask(task.Code)
	assert.NoError(t, err)
	assert.True(t, asyncResult.GetState().IsCanceled())
	_, err = asyncResult.Get(100 * time.Millisecond)
	assert.Error(t, err)
	assert.Equal(t, "canceled", err.Error())
	//time.Sleep(time.Minute)
	//value, err := redisServer.Get(key)
	//assert.NoError(t, err)
	//assert.Equal(t, "0", value)
}

func TestCancelPeriodicChain(t *testing.T) {
	key := "test_cancel_periodic_chain"
	err := redisServer.Set(key, "0")
	assert.NoError(t, err)
	task1, task2, task3 := newPeriodicTask(key, 1), newPeriodicTask(key, 2), newPeriodicTask(key, 3)
	chain, err := tasks.NewChain(task1, task2, task3)
	assert.NoError(t, err)
	asyncResult, err := server.SendPeriodicChain(Spec, chain)
	assert.NoError(t, err)
	err = server.CancelPeriodicTask(chain.Tasks[0].Code)
	assert.NoError(t, err)
	_, err = asyncResult.Get(100 * time.Millisecond)
	assert.Error(t, err)
	assert.Equal(t, "canceled", err.Error())
	//time.Sleep(time.Minute)
	//value, err := redisServer.Get(key)
	//assert.NoError(t, err)
	//assert.Equal(t, "0", value)
}

func TestCancelPeriodicGroup(t *testing.T) {
	key := "test_cancel_periodic_group"
	err := redisServer.Set(key, "0")
	assert.NoError(t, err)
	task1, task2, task3 := newPeriodicTask(key, 1), newPeriodicTask(key, 2), newPeriodicTask(key, 3)
	group, err := tasks.NewGroup(task1, task2, task3)
	assert.NoError(t, err)
	asyncResults, err := server.SendPeriodicGroup(Spec, group, 10)
	assert.NoError(t, err)
	err = server.CancelPeriodicTask(group.Tasks[0].Code)
	assert.NoError(t, err)
	for _, asyncResult := range asyncResults {
		_, err = asyncResult.Get(100 * time.Millisecond)
		assert.Error(t, err)
		assert.Equal(t, "canceled", err.Error())
	}
	//time.Sleep(time.Minute)
	//value, err := redisServer.Get(key)
	//assert.NoError(t, err)
	//assert.Equal(t, "0", value)
}

func TestCancelPeriodicChord(t *testing.T) {
	key := "test_cancel_periodic_chord"
	err := redisServer.Set(key, "0")
	assert.NoError(t, err)
	task1, task2, task3 := newPeriodicTask(key, 1), newPeriodicTask(key, 2), newPeriodicTask(key, 3)
	task4 := newPeriodicTask(key, 10)
	group, err := tasks.NewGroup(task1, task2, task3)
	assert.NoError(t, err)
	chord, err := tasks.NewChord(group, task4)
	assert.NoError(t, err)
	asyncResult, err := server.SendPeriodicChord(Spec, chord, 10)
	assert.NoError(t, err)
	err = server.CancelPeriodicTask(chord.Group.Tasks[0].Code)
	assert.NoError(t, err)
	_, err = asyncResult.Get(100 * time.Millisecond)
	assert.Error(t, err)
	assert.Equal(t, "canceled", err.Error())
	//time.Sleep(time.Minute)
	//value, err := redisServer.Get(key)
	//assert.NoError(t, err)
	//assert.Equal(t, "0", value)
}

func registerRedisTestTasks(server *vecna.Server) {
	tasks := map[string]interface{}{
		"periodic_test": func(key string, delta int) (int, error) {
			return redisServer.Incr(key, delta)
		},
	}

	server.RegisterTasks(tasks)
}

func newDelayedTask(key string, delta int) *tasks.Signature {
	eta := time.Now().Add(2 * time.Second)
	return &tasks.Signature{
		Name: "delayed_test",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: key,
			},
			{
				Type:  "int",
				Value: delta,
			},
		},
		ETA: &eta,
	}
}

func newPeriodicTask(key string, delta int) *tasks.Signature {
	return &tasks.Signature{
		Name: "periodic_test",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: key,
			},
			{
				Type:  "int",
				Value: delta,
			},
		},
		Immutable: true,
	}
}

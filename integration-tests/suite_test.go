package integration_tests

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/imasker/vecna"
	"github.com/imasker/vecna/tasks"
)

type ascendingInt64s []int64

func (a ascendingInt64s) Len() int {
	return len(a)
}

func (a ascendingInt64s) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ascendingInt64s) Less(i, j int) bool {
	return a[i] < a[j]
}

const Spec = "* * * * *"

// testAll for testing instance of different data source
func testAll(t *testing.T, server *vecna.Server) {
	testSendTask(t, server)
	testSendGroup(t, server, 0) // with unlimited concurrency
	testSendGroup(t, server, 2) // with limited concurrency (2 parallel tasks at the most)
	testSendChord(t, server)
	testSendChord2(t, server)
	testSendChain(t, server)
	testReturnJustError(t, server)
	testReturnMultipleValues(t, server)
	testPanic(t, server)
	testDelay(t, server)
	testDelay2(t, server)
}

func testSendTask(t *testing.T, server *vecna.Server) {
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

func testSendGroup(t *testing.T, server *vecna.Server, sendConcurrency int) {
	t1, t2, t3 := newAddTask(1, 1), newAddTask(2, 2), newAddTask(5, 6)

	group, _ := tasks.NewGroup(t1, t2, t3)

	asyncResults, err := server.SendGroup(group, sendConcurrency)
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

func testSendChain(t *testing.T, server *vecna.Server) {
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

func testSendChord(t *testing.T, server *vecna.Server) {
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

func testReturnJustError(t *testing.T, server *vecna.Server) {
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

func testReturnMultipleValues(t *testing.T, server *vecna.Server) {
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

func testPanic(t *testing.T, server *vecna.Server) {
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

func testDelay(t *testing.T, server *vecna.Server) {
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

func testDelay2(t *testing.T, server *vecna.Server) {
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

func testSendChord2(t *testing.T, server *vecna.Server) {
	task1, task2, task3 := tasks.Signature{Name: "periodic_chord_test"}, tasks.Signature{Name: "periodic_chord_test"}, tasks.Signature{Name: "periodic_chord_test"}
	task4 := tasks.Signature{Name: "periodic_test", Immutable: true}
	group, err := tasks.NewGroup(&task1, &task2, &task3)
	assert.NoError(t, err)
	chord, err := tasks.NewChord(group, &task4)
	assert.NoError(t, err)
	chordAsyncResult, err := server.SendChord(chord, 10)
	assert.NoError(t, err)
	results, err := chordAsyncResult.Get(5 * time.Millisecond)
	assert.NoError(t, err)
	if len(results) != 1 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
	}
	assert.Greater(t, results[0].Interface(), int64(0))
}

func registerTestTasks(server *vecna.Server) {
	tasks := map[string]interface{}{
		"add": func(args ...int64) (int64, error) {
			sum := int64(0)
			for _, arg := range args {
				sum += arg
			}
			return sum, nil
		},
		"multiply": func(args ...int64) (int64, error) {
			sum := int64(1)
			for _, arg := range args {
				sum *= arg
			}
			return sum, nil
		},
		"sum": func(numbers []int64) (int64, error) {
			var sum int64
			for _, num := range numbers {
				sum += num
			}
			return sum, nil
		},
		"return_just_error": func(msg string, fail bool) (err error) {
			if fail {
				err = errors.New(msg)
			}
			return err
		},
		"return_multiple_values": func(arg1, arg2 string, fail bool) (r1 string, r2 string, err error) {
			if fail {
				err = errors.New("some error")
			} else {
				r1 = arg1
				r2 = arg2
			}
			return r1, r2, err
		},
		"panic": func() (string, error) {
			panic(errors.New("oops"))
		},
		"delay_test": func() (int64, error) {
			return time.Now().UTC().UnixNano(), nil
		},
		"periodic_test": func() (int64, error) {
			now := time.Now()
			fmt.Println(now.String())
			return now.UTC().UnixNano(), nil
		},
		"periodic_chain_test": func() error {
			fmt.Println(time.Now().String() + " chain")
			return nil
		},
		"periodic_group_test": func() error {
			fmt.Println(time.Now().String() + " group")
			return nil
		},
		"periodic_chord_test": func() error {
			fmt.Println(time.Now().String() + " chord")
			return nil
		},
		"periodic_chord_callback": func() (int64, error) {
			now := time.Now()
			fmt.Println(now.String() + " chord_callback1")
			return now.UTC().UnixNano(), nil
		},
	}

	server.RegisterTasks(tasks)
}

func newAddTask(a, b int) *tasks.Signature {
	return &tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: a,
			},
			{
				Type:  "int64",
				Value: b,
			},
		},
	}
}

func newMultipleTask(nums ...int) *tasks.Signature {
	args := make([]tasks.Arg, len(nums))
	for i, n := range nums {
		args[i] = tasks.Arg{
			Type:  "int64",
			Value: n,
		}
	}
	return &tasks.Signature{
		Name: "multiply",
		Args: args,
	}
}

func newSumTask(nums []int64) *tasks.Signature {
	return &tasks.Signature{
		Name: "sum",
		Args: []tasks.Arg{
			{
				Type:  "[]int64",
				Value: nums,
			},
		},
	}
}

func newErrorTask(msg string, fail bool) *tasks.Signature {
	return &tasks.Signature{
		Name: "return_just_error",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: msg,
			},
			{
				Type:  "bool",
				Value: fail,
			},
		},
	}
}

func newMultipleReturnTask(arg1, arg2 string, fail bool) *tasks.Signature {
	return &tasks.Signature{
		Name: "return_multiple_values",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: arg1,
			},
			{
				Type:  "string",
				Value: arg2,
			},
			{
				Type:  "bool",
				Value: fail,
			},
		},
	}
}

func newDelayTask(eta time.Time) *tasks.Signature {
	return &tasks.Signature{
		Name: "delay_test",
		ETA:  &eta,
	}
}

package tasks

import (
	"errors"
	"reflect"
)

var (
	// ErrTaskMustBeFunc ...
	ErrTaskMustBeFunc = errors.New("task must be a func type")
	// ErrTaskReturnsNoValue ...
	ErrTaskReturnsNoValue = errors.New("task must return at least one single value")
	// ErrLastReturnValueMustBeError ...
	ErrLastReutrnValueMustBeError = errors.New("last return value of a task must be error")
)

func ValidateTask(task interface{}) error {
	v := reflect.ValueOf(task)
	t := v.Type()

	// Task must be a function
	if t.Kind() != reflect.Func {
		return ErrTaskMustBeFunc
	}

	// Task must return at least one single value
	if t.NumOut() < 1 {
		return ErrTaskReturnsNoValue
	}

	// Last return value must be error
	lastReturnType := t.Out(t.NumOut() - 1)
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	if !lastReturnType.Implements(errorInterface) {
		return ErrLastReutrnValueMustBeError
	}

	return nil
}

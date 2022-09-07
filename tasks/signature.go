package tasks

import (
	"fmt"
	"time"

	gonanoid "github.com/matoous/go-nanoid/v2"

	"github.com/imasker/vecna/utils"
)

// Arg represents a single argument passed to in vocation fo a task
type Arg struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

// Headers represents the headers which should be used to direct the task
type Headers map[string]interface{}

// Set on Headers implements opentracing.TextMapWriter for trace propagation
func (h Headers) Set(key, val string) {
	h[key] = val
}

// ForeachKey on Headers implements opentracing.TextMapReader for trace propagation.
// It is essentially the same as the opentracing.TextMapReader implementation except
// for the added casting from interface{} to string.
func (h Headers) ForeachKey(handler func(key, val string) error) error {
	for k, v := range h {
		// Skip any non string values
		stringValue, ok := v.(string)
		if !ok {
			continue
		}

		if err := handler(k, stringValue); err != nil {
			return err
		}
	}

	return nil
}

// Signature represents a single task invocation
type Signature struct {
	ID             string
	Name           string
	RoutingKey     string
	ETA            *time.Time
	GroupID        string
	GroupTaskCount int
	Args           []Arg
	Headers        Headers
	Priority       uint8
	Immutable      bool
	RetryCount     int
	RetryTimeout   int
	OnSuccess      []*Signature
	OnError        []*Signature
	ChordCallback  *Signature
	// IgnoreWhenTaskNotRegistered auto removes the request when there is no handler available
	// When this is true a task with no handler will be ignored and not placed back in the queue
	IgnoreWhenTaskNotRegistered bool
	// Spec marks this task being a periodic task. It requires 5 entries representing: minute,
	// hour, day of month, month and day of week, in that order.
	Spec string
	// Code every task in group or in periodic tasks has the same code, so that we can cancel the
	// task by using this code. By default, code will be the GroupID if exists or ID. It can also
	// be specified by the user.
	Code string
}

func NewSignature(name string, args []Arg) (*Signature, error) {
	signatureID, _ := gonanoid.New()
	return &Signature{
		ID:   fmt.Sprintf("task_%s", signatureID),
		Name: name,
		Args: args,
	}, nil
}

func CopySignatures(signatures ...*Signature) []*Signature {
	var sigs = make([]*Signature, len(signatures))
	for index, signature := range signatures {
		sigs[index] = CopySignature(signature)
	}
	return sigs
}

func CopySignature(signature *Signature) *Signature {
	var sig = new(Signature)
	_ = utils.DeepCopy(sig, signature)
	return sig
}

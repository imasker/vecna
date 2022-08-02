package tasks

import (
	"fmt"
	"time"
	"vecna/utils"

	gonanoid "github.com/matoous/go-nanoid/v2"
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

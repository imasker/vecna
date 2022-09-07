package iface

import (
	"context"

	"github.com/imasker/vecna/config"
	"github.com/imasker/vecna/tasks"
)

// Broker - a common interface for all brokers
type Broker interface {
	GetConfig() *config.Config
	SetRegisteredTaskNames(names []string)
	IsTaskRegistered(name string) bool
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
	StopConsuming()
	Publish(ctx context.Context, task *tasks.Signature) error
	GetPendingTasks(queue string) ([]*tasks.Signature, error)
	GetDelayedTasks() ([]*tasks.Signature, error)
	AdjustRoutingKey(s *tasks.Signature)
	PublishPeriodicTask(signature *tasks.Signature, group *tasks.Group, chord *tasks.Chord) error
	GetPeriodicTask(code string, next bool) (*tasks.Signature, error)
	GetPeriodicGroup(code string, next bool) (*tasks.Group, error)
	GetPeriodicChord(code string, next bool) (*tasks.Chord, error)
	RemovePeriodicTask(code string) error
	RemoveDelayedTasks(signatureIDs ...string) error
}

// TaskProcessor - can process a delivered task
// This will probably always be a worker instance
type TaskProcessor interface {
	Process(signature *tasks.Signature) error
	CustomQueue() string
	PreConsumeHandler() bool
}

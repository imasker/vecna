package config

import (
	"time"
)

const (
	// DefaultResultsExpireIn is a default time used to expire task states and group metadata from the backend
	DefaultResultsExpireIn = 3600
)

var (
	// Start with sensible default values
	defaultCnf = &Config{
		Broker:          "redis://localhost:6379/0",
		DefaultQueue:    "vecna_tasks",
		ResultBackend:   "redis://localhost:6379/0",
		ResultsExpireIn: DefaultResultsExpireIn,
		Redis: &RedisConfig{
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
		},
		DefaultSendConcurrency: 100,
	}

	reloadDelay = time.Second * 10
)

// Config holds all configuration for our program
type Config struct {
	Broker        string `yaml:"broker" envconfig:"BROKER"`
	Lock          string `yaml:"lock" envconfig:"LOCK"`
	DefaultQueue  string `yaml:"default_queue" envconfig:"DEFAULT_QUEUE"`
	ResultBackend string `yaml:"result_backend" envconfig:"RESULT_BACKEND"`
	// todo: make sure that groupMeta and taskState won't be deleted when they are required
	ResultsExpireIn int          `yaml:"results_expire_in" envconfig:"RESULTS_EXPIRE_IN"`
	Redis           *RedisConfig `yaml:"redis"`
	// NoUnixSignals - when set disables signal handling in vecna
	NoUnixSignals          bool `yaml:"no_unix_signals" envconfig:"NO_UNIX_SIGNALS"`
	DefaultSendConcurrency int  `yaml:"default_send_concurrency" envconfig:"DEFAULT_SEND_CONCURRENCY"`
}

// RedisConfig ...
type RedisConfig struct {
	// NormalTasksPollPeriod specifies the period in milliseconds when polling redis for normal tasks
	// Default: 1000
	NormalTasksPollPeriod int `yaml:"normal_tasks_poll_period" envconfig:"REDIS_NORMAL_TASKS_POLL_PERIOD"`

	// DelayedTasksPollPeriod specifies the period in milliseconds when polling redis for delayed tasks
	// Default: 20
	DelayedTasksPollPeriod int    `yaml:"delayed_tasks_poll_period" envconfig:"REDIS_DELAYED_TASKS_POLL_PERIOD"`
	DelayedTasksKey        string `yaml:"delayed_tasks_key" envconfig:"REDIS_DELAYED_TASKS_KEY"`
	DelayedTasksCacheKey   string `yaml:"delayed_tasks_cache_key" envconfig:"REDIS_DELAYED_TASKS_CACHE_KEY"`

	// PeriodicTasksKey ...
	PeriodicTasksKey string `yaml:"periodic_tasks_key" envconfig:"REDIS_PERIODIC_TASKS_KEY"`
	// CanceledTasksKey ...
	CanceledTasksKey string `yaml:"canceled_tasks_key" envconfig:"REDIS_CANCELEDTASKSKEY"`

	// MasterName specifies a redis master name in order to configure a sentinel-backed redis FailoverClient
	MasterName string `yaml:"master_name" envconfig:"REDIS_MASTER_NAME"`
}

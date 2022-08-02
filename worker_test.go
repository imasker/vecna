package vecna_test

import (
	"testing"
	"vecna"

	"github.com/stretchr/testify/assert"
)

func TestRedactURL(t *testing.T) {
	t.Parallel()

	broker := "redis://:password@localhost:6379/0"
	redactedURL := vecna.RedactURL(broker)
	assert.Equal(t, "redis://localhost:5672", redactedURL)
}

func TestWorker_PreConsumeHandler(t *testing.T) {
	t.Parallel()

	worker := &vecna.Worker{}

	worker.SetPreConsumeHandler(SamplePreConsumeHandler)
	assert.True(t, worker.PreConsumeHandler())
}

func SamplePreConsumeHandler(worker *vecna.Worker) bool {
	return true
}

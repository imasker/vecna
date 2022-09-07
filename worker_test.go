package vecna_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/imasker/vecna"
)

func TestRedactURL(t *testing.T) {
	t.Parallel()

	broker := "redis://:password@localhost:6379/0"
	redactedURL := vecna.RedactURL(broker)
	assert.Equal(t, "redis://localhost:6379", redactedURL)
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

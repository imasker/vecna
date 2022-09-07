package tasks_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/imasker/vecna/tasks"
)

func TestNewChain(t *testing.T) {
	t.Parallel()

	task1 := tasks.Signature{
		Name: "foo",
		Args: []tasks.Arg{
			{
				Type:  "float64",
				Value: 1,
			},
			{
				Type:  "float64",
				Value: 1,
			},
		},
	}

	task2 := tasks.Signature{
		Name: "bar",
		Args: []tasks.Arg{
			{
				Type:  "float64",
				Value: 5,
			},
			{
				Type:  "float64",
				Value: 6,
			},
		},
	}

	task3 := tasks.Signature{
		Name: "qux",
		Args: []tasks.Arg{
			{
				Type:  "float64",
				Value: 4,
			},
		},
	}

	chain, _ := tasks.NewChain(&task1, &task2, &task3)

	firstTask := chain.Tasks[0]

	assert.Equal(t, "foo", firstTask.Name)
	assert.Equal(t, "bar", firstTask.OnSuccess[0].Name)
	assert.Equal(t, "qux", firstTask.OnSuccess[0].OnSuccess[0].Name)
}

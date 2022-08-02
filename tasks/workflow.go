package tasks

import (
	"vecna/utils"
)

// Chain creates a chain of tasks to be executed one after another
type Chain struct {
	Tasks []*Signature
}

// Group creates a set of tasks to be executed in parallel
type Group struct {
	GroupID string
	Tasks   []*Signature
}

// Chord adds an optional callback to the group to be executed
// after all tasks in the group finished
type Chord struct {
	Group    *Group
	Callback *Signature
}

// GetIDs returns slice of task IDs
func (g *Group) GetIDs() []string {
	taskIDs := make([]string, len(g.Tasks))
	for i, signature := range g.Tasks {
		taskIDs[i] = signature.ID
	}
	return taskIDs
}

// NewChain creates a new chain of tasks to be processed one by one, passing
// results unless task signatures are set to be immutable
func NewChain(signatures ...*Signature) *Chain {
	// Auto generate task IDs if needed
	for _, signature := range signatures {
		if signature.ID == "" {
			signature.ID = utils.GenerateID("task_")
		}
	}

	for i := len(signatures) - 1; i > 0; i-- {
		if i > 0 {
			signatures[i-1].OnSuccess = []*Signature{signatures[i]}
		}
	}

	chain := &Chain{Tasks: signatures}

	return chain
}

// NewGroup creates a new group of tasks to be processed in parallel
func NewGroup(signatures ...*Signature) *Group {
	// Generate a group ID
	groupID := utils.GenerateID("group_")

	// Auto generate task IDs if needed, group tasks by common group ID
	for _, signature := range signatures {
		if signature.ID == "" {
			signature.ID = utils.GenerateID("task_")
		}
		signature.GroupID = groupID
		signature.GroupTaskCount = len(signatures)
	}

	return &Group{
		GroupID: groupID,
		Tasks:   signatures,
	}
}

// NewChord creates a new chord (a group of tasks with a single callback
// to be executed after all tasks in the group has completed)
func NewChord(group *Group, callback *Signature) *Chord {
	if callback.ID == "" {
		// Generate a ID for the chord callback
		callback.ID = utils.GenerateID("chord_")
	}

	// Add a chord callback to all tasks
	for _, signature := range group.Tasks {
		signature.ChordCallback = callback
	}

	return &Chord{Group: group, Callback: callback}
}

package eventstore

import "context"

// Enum of all possible ProjectionStatus
type Status string

const (
	StatusIdle                        Status = "idle"
	StatusRunning                     Status = "running"
	StatusStopping                    Status = "stopping"
	StatusResetting                   Status = "resetting"
	StatusDeleting                    Status = "deleting"
	StatusDeletingIinclEmittedEevents Status = "deleting incl emitted events"
)

// StreamProjection is used if you want to Projection over different Streams with different Filters
type StreamProjection struct {
	// StreamName of the EventStream
	StreamName string
	// Matcher a optional list of custom filters
	Matcher MetadataMatcher
}

// EventHandler process a single Event and returns the new ProjectionState
// The First argument is the current state of the projection
// The Second argument is the loaded event
type EventHandler = func(state interface{}, event DomainEvent) interface{}

// ProjectionManager manages the Projections Table / Collection and hase multiple implementations for different persistens layers
type ProjectionManager interface {
	// FetchProjectionStatus returns the active status of the given projection
	FetchProjectionStatus(ctx context.Context, projectionName string) (Status, error)
	// CreateProjection creates a new projections entry in the projections table
	CreateProjection(ctx context.Context, projectionName string, state interface{}, status Status) error
	// DeleteProjection deletes a projection entry from the projections table
	DeleteProjection(ctx context.Context, projectionName string) error
	// ResetProjection resets state and positions from the given projection
	ResetProjection(ctx context.Context, projectionName string, state interface{}) error
	// PersistProjection persists the current state and position of the given projection
	PersistProjection(ctx context.Context, projectionName string, state interface{}, streamPositions map[string]int) error
	// LoadProjection loads latest state and positions of the given projection
	LoadProjection(ctx context.Context, projectionName string) (map[string]int, interface{}, error)
	// UpdateProjectionStatus updates the status of a given projection
	UpdateProjectionStatus(ctx context.Context, projectionName string, status Status) error
	// ProjectionExists returns if a projection with the given name exists
	ProjectionExists(ctx context.Context, projectionName string) (bool, error)
}

// FindStatusInSlice returns if the given status is in the given status slice
func FindStatusInSlice(slice []Status, val Status) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}

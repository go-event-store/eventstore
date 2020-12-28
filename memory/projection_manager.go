package memory

import (
	"context"
	"sync"

	eventstore "github.com/go-event-store/eventstore"
)

type projection struct {
	name     string
	position map[string]int
	state    interface{}
	status   eventstore.Status
}

type ProjectionManager struct {
	projections map[string]*projection
	mutex       *sync.Mutex
}

func (pm ProjectionManager) FetchProjectionStatus(_ context.Context, projectionName string) (eventstore.Status, error) {
	projection, ok := pm.projections[projectionName]
	if !ok {
		return eventstore.StatusIdle, eventstore.ProjectionNotFound{Name: projectionName}
	}

	return projection.status, nil
}

func (pm ProjectionManager) CreateProjection(_ context.Context, projectionName string, state interface{}, status eventstore.Status) error {
	pm.mutex.Lock()
	pm.projections[projectionName] = &projection{
		name:     projectionName,
		position: map[string]int{},
		state:    state,
		status:   status,
	}
	pm.mutex.Unlock()
	return nil
}

func (pm ProjectionManager) DeleteProjection(_ context.Context, projectionName string) error {
	pm.mutex.Lock()
	delete(pm.projections, projectionName)
	pm.mutex.Unlock()
	return nil
}

func (pm ProjectionManager) ResetProjection(_ context.Context, projectionName string, state interface{}) error {
	pm.mutex.Lock()
	pm.projections[projectionName] = &projection{
		name:     projectionName,
		position: map[string]int{},
		state:    state,
		status:   eventstore.StatusIdle,
	}
	pm.mutex.Unlock()
	return nil
}

func (pm ProjectionManager) PersistProjection(_ context.Context, projectionName string, state interface{}, streamPositions map[string]int) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	projection, ok := pm.projections[projectionName]
	if !ok {
		return eventstore.ProjectionNotFound{Name: projectionName}
	}

	projection.state = state
	projection.position = streamPositions

	return nil
}

func (pm ProjectionManager) UpdateProjectionStatus(_ context.Context, projectionName string, status eventstore.Status) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()
	projection, ok := pm.projections[projectionName]
	if !ok {
		return eventstore.ProjectionNotFound{Name: projectionName}
	}

	projection.status = status

	return nil
}

func (pm ProjectionManager) LoadProjection(_ context.Context, projectionName string) (map[string]int, interface{}, error) {
	projection, ok := pm.projections[projectionName]
	if !ok {
		return map[string]int{}, nil, eventstore.ProjectionNotFound{Name: projectionName}
	}

	return projection.position, projection.state, nil
}

func (pm ProjectionManager) ProjectionExists(_ context.Context, projectionName string) (bool, error) {
	_, ok := pm.projections[projectionName]

	return ok, nil
}

func NewProjectionManager() *ProjectionManager {
	return &ProjectionManager{
		projections: map[string]*projection{},
		mutex:       new(sync.Mutex),
	}
}

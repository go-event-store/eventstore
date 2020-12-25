package eventstore

import (
	"context"
	"time"
)

// Projector creates a custom ReadModel over one or multiple streams
type ReadModelProjector struct {
	state            interface{}
	name             string
	status           Status
	eventStore       *EventStore
	ReadModel        ReadModel
	manager          ProjectionManager
	initHandler      func() interface{}
	handler          func(state interface{}, event DomainEvent) interface{}
	handlers         map[string]func(state interface{}, event DomainEvent) interface{}
	metadataMatchers map[string]MetadataMatcher
	streamPositions  map[string]int
	persistBlockSize int
	eventCounter     int
	isStopped        bool

	query struct {
		all     bool
		streams []string
	}
}

// Init the state, define the type and/or prefill it with data
func (q *ReadModelProjector) Init(handler func() interface{}) *ReadModelProjector {
	if q.initHandler != nil {
		panic(ProjectorAlreadyInitialized())
	}

	q.initHandler = handler
	q.state = handler()

	return q
}

// FromAll read events from all existing EventStreams
func (q *ReadModelProjector) FromAll() *ReadModelProjector {
	if q.query.all || len(q.query.streams) > 0 {
		panic(ProjectorFromWasAlreadyCalled())
	}

	q.query.all = true

	return q
}

// FromStream read events from a single EventStream
func (q *ReadModelProjector) FromStream(streamName string, matcher MetadataMatcher) *ReadModelProjector {
	if q.query.all || len(q.query.streams) > 0 {
		panic(ProjectorFromWasAlreadyCalled())
	}

	q.query.streams = append(q.query.streams, streamName)
	q.metadataMatchers[streamName] = matcher

	return q
}

// FromStreams read events from multiple EventStreams
func (q *ReadModelProjector) FromStreams(streams ...StreamProjection) *ReadModelProjector {
	if q.query.all || len(q.query.streams) > 0 {
		panic(ProjectorFromWasAlreadyCalled())
	}

	for _, stream := range streams {
		q.query.streams = append(q.query.streams, stream.StreamName)
		q.metadataMatchers[stream.StreamName] = stream.Matcher
	}

	return q
}

// When define multiple handlers for
// You can create one handler for one event
// Events without a handler will not be progressed
func (q *ReadModelProjector) When(handlers map[string]EventHandler) *ReadModelProjector {
	if q.handler != nil || len(q.handlers) != 0 {
		panic(ProjectorFromWasAlreadyCalled())
	}

	q.handlers = handlers

	return q
}

// WhenAny defines a single handler for all possible Events
func (q *ReadModelProjector) WhenAny(handler EventHandler) *ReadModelProjector {
	if q.handler != nil || len(q.handlers) != 0 {
		panic(ProjectorFromWasAlreadyCalled())
	}

	q.handler = handler

	return q
}

// Delete the ReadModelProjection from the Projections table / collection and if deleteProjection is true
// it also runs the Delete Method of your ReadModel
func (q *ReadModelProjector) Delete(ctx context.Context, deleteProjection bool) error {
	err := q.manager.DeleteProjection(ctx, q.name)
	if err != nil {
		return err
	}

	if deleteProjection {
		err = q.ReadModel.Delete(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Reset the ReadModelProjection state and EventStream positions
// Run also the Reset Method of your ReadModel
func (q *ReadModelProjector) Reset(ctx context.Context) error {
	q.streamPositions = map[string]int{}
	q.state = struct{}{}

	err := q.ReadModel.Reset(ctx)
	if err != nil {
		return err
	}

	if q.initHandler != nil {
		q.state = q.initHandler()
	}

	return q.manager.ResetProjection(ctx, q.name, q.state)
}

// Stop the ReadModelProjection and persist the current state and EventStream positions
func (q *ReadModelProjector) Stop(ctx context.Context) error {
	err := q.persist(ctx)
	if err != nil {
		return err
	}

	q.status = StatusIdle

	err = q.manager.UpdateProjectionStatus(ctx, q.name, StatusIdle)
	if err != nil {
		return err
	}

	q.isStopped = true

	return nil
}

// Run the ReadModelProjection
func (q *ReadModelProjector) Run(ctx context.Context, keepRunning bool) error {
	if q.handler == nil && len(q.handlers) == 0 {
		panic(ProjectorNoHandler())
	}

	if q.state == nil {
		panic(ProjectorStateNotInitialised())
	}

	var err error

	switch q.fetchRemoteStatus(ctx) {
	case StatusStopping:
		err = q.load(ctx)
		if err != nil {
			return err
		}

		err = q.Stop(ctx)
		if err != nil {
			return err
		}
	case StatusDeleting:
		err = q.Delete(ctx, true)
		if err != nil {
			return err
		}
	case StatusResetting:
		err = q.Reset(ctx)
		if err != nil {
			return err
		}

		if keepRunning {
			q.startAgain(ctx)
		}
	}

	if ok, err := q.manager.ProjectionExists(ctx, q.name); ok == false {
		if err != nil {
			return err
		}

		err = q.manager.CreateProjection(ctx, q.name, q.state, q.status)
		if err != nil {
			return err
		}
	}

	if ok, err := q.ReadModel.IsInitialized(ctx); ok == false {
		if err != nil {
			return err
		}

		err = q.ReadModel.Init(ctx)
		if err != nil {
			return err
		}
	}

	err = q.prepareStreamPosition(ctx)
	if err != nil {
		return err
	}

	err = q.load(ctx)
	if err != nil {
		return err
	}

	q.isStopped = false

	for {
		events, err := q.retreiveEventsFromStream(ctx)
		if err != nil {
			return err
		}

		err = q.handleStream(ctx, events)
		if err != nil {
			return err
		}

		if q.eventCounter > 0 {
			err := q.persist(ctx)
			if err != nil {
				return err
			}
		}

		q.eventCounter = 0

		switch q.fetchRemoteStatus(ctx) {
		case StatusStopping:
			err = q.Stop(ctx)
			if err != nil {
				return err
			}
		case StatusDeleting:
			err = q.Delete(ctx, false)
			if err != nil {
				return err
			}
		case StatusDeletingIinclEmittedEevents:
			err = q.Delete(ctx, true)
			if err != nil {
				return err
			}
		case StatusResetting:
			err = q.Reset(ctx)
			if err != nil {
				return err
			}

			if keepRunning {
				err = q.startAgain(ctx)
				if err != nil {
					return err
				}
			}
		}

		if !keepRunning || q.isStopped {
			break
		}

		time.Sleep(200 * time.Millisecond)
	}

	return nil
}

// State returns the current ReadModelProjection State
func (q ReadModelProjector) State() interface{} {
	return q.state
}

// Name returns the current ReadModelProjection Name
func (q ReadModelProjector) Name() string {
	return q.name
}

// Status returns the current ReadModelProjection Status
func (q ReadModelProjector) Status() Status {
	return q.status
}

func (q *ReadModelProjector) startAgain(ctx context.Context) error {
	q.isStopped = false

	err := q.manager.UpdateProjectionStatus(ctx, q.name, StatusRunning)
	if err != nil {
		return err
	}

	q.status = StatusRunning

	return nil
}

func (q *ReadModelProjector) load(ctx context.Context) error {
	positions, state, err := q.manager.LoadProjection(ctx, q.name)
	if err != nil {
		return err
	}

	for stream, position := range positions {
		q.streamPositions[stream] = position
	}

	q.state = state

	return nil
}

func (q *ReadModelProjector) persist(ctx context.Context) error {
	err := q.ReadModel.Persist(ctx)
	if err != nil {
		return err
	}

	err = q.manager.PersistProjection(ctx, q.name, q.state, q.streamPositions)
	if err != nil {
		return err
	}

	return nil
}

func (q *ReadModelProjector) persistAndFetchRemoteStatusWhenBlockSizeThresholdReached(ctx context.Context) error {
	if q.eventCounter != q.persistBlockSize {
		return nil
	}

	err := q.persist(ctx)
	if err != nil {
		return err
	}

	q.eventCounter = 0
	q.status = q.fetchRemoteStatus(ctx)

	if _, ok := FindStatusInSlice([]Status{StatusIdle, StatusRunning}, q.status); ok == false {
		q.isStopped = true
	}

	return nil
}

func (q *ReadModelProjector) prepareStreamPosition(ctx context.Context) error {
	var streams []string
	var err error

	if q.query.all {
		streams, err = q.eventStore.FetchAllStreamNames(ctx)
		if err != nil {
			return err
		}
	} else {
		streams = q.query.streams
	}

	for _, stream := range streams {
		if _, ok := q.streamPositions[stream]; ok == false {
			q.streamPositions[stream] = 0
		}
	}

	return nil
}

func (q *ReadModelProjector) fetchRemoteStatus(ctx context.Context) Status {
	status, err := q.manager.FetchProjectionStatus(ctx, q.name)
	if err != nil {
		return StatusRunning
	}

	return status
}

func (q *ReadModelProjector) retreiveEventsFromStream(ctx context.Context) (DomainEventIterator, error) {
	streams := []LoadStreamParameter{}

	for stream, position := range q.streamPositions {
		streams = append(streams, LoadStreamParameter{StreamName: stream, FromNumber: position + 1, Matcher: q.metadataMatchers[stream]})
	}

	return q.eventStore.MergeAndLoad(ctx, 0, streams...)
}

func (q *ReadModelProjector) handleStream(ctx context.Context, events DomainEventIterator) error {
	for events.Next() {
		event, err := events.Current()
		if err != nil {
			return err
		}

		q.streamPositions[event.Metadata()["stream"].(string)] = event.Number()
		q.eventCounter++

		if q.handler != nil {
			q.state = q.handler(q.state, *event)
		}

		if handler, ok := q.handlers[event.Name()]; ok {
			q.state = handler(q.state, *event)
		}

		err = q.persistAndFetchRemoteStatusWhenBlockSizeThresholdReached(ctx)
		if err != nil {
			return err
		}

		if q.isStopped {
			break
		}
	}

	return nil
}

// NewReadModelProjector for the given ReadModel implementation, EventStore and ProjectionManager
// Find an example for a ReadModel in example/read_model.go
func NewReadModelProjector(name string, readModel ReadModel, eventStore *EventStore, manager ProjectionManager) ReadModelProjector {
	return ReadModelProjector{
		name:             name,
		state:            nil,
		status:           StatusIdle,
		ReadModel:        readModel,
		eventStore:       eventStore,
		manager:          manager,
		initHandler:      nil,
		handler:          nil,
		handlers:         map[string]func(state interface{}, event DomainEvent) interface{}{},
		metadataMatchers: map[string]MetadataMatcher{},
		streamPositions:  map[string]int{},
		isStopped:        false,
		eventCounter:     0,
		persistBlockSize: 1000,
	}
}

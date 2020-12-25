package eventstore

import (
	"context"
	"time"
)

// Projector creates a persistened projection of one or multiple streams
type Projector struct {
	state            interface{}
	name             string
	status           Status
	eventStore       *EventStore
	manager          ProjectionManager
	initHandler      func() interface{}
	handler          func(state interface{}, event DomainEvent) interface{}
	handlers         map[string]func(state interface{}, event DomainEvent) interface{}
	metadataMatchers map[string]MetadataMatcher
	streamPositions  map[string]int
	persistBlockSize int
	eventCounter     int
	streamCreated    bool
	isStopped        bool

	query struct {
		all     bool
		streams []string
	}
}

// Init the state, define the type and/or prefill it with data
func (q *Projector) Init(handler func() interface{}) *Projector {
	if q.initHandler != nil {
		panic(ProjectorAlreadyInitialized())
	}

	q.initHandler = handler
	q.state = handler()

	return q
}

// FromAll read events from all existing EventStreams
func (q *Projector) FromAll() *Projector {
	if q.query.all || len(q.query.streams) > 0 {
		panic(ProjectorFromWasAlreadyCalled())
	}

	q.query.all = true

	return q
}

// FromStream read events from a single EventStream
func (q *Projector) FromStream(streamName string, matcher MetadataMatcher) *Projector {
	if q.query.all || len(q.query.streams) > 0 {
		panic(ProjectorFromWasAlreadyCalled())
	}

	q.query.streams = append(q.query.streams, streamName)
	q.metadataMatchers[streamName] = matcher

	return q
}

// FromStreams read events from multiple EventStreams
func (q *Projector) FromStreams(streams ...StreamProjection) *Projector {
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
func (q *Projector) When(handlers map[string]EventHandler) *Projector {
	if q.handler != nil || len(q.handlers) != 0 {
		panic(ProjectorFromWasAlreadyCalled())
	}

	q.handlers = handlers

	return q
}

// WhenAny defines a single handler for all possible Events
func (q *Projector) WhenAny(handler EventHandler) *Projector {
	if q.handler != nil || len(q.handlers) != 0 {
		panic(ProjectorFromWasAlreadyCalled())
	}

	q.handler = handler

	return q
}

// Emit creates a new EventStream with the name of the Projection
// And append the event to this new EventStream
func (q *Projector) Emit(ctx context.Context, event DomainEvent) error {
	if ok, _ := q.eventStore.HasStream(ctx, q.name); ok == false && q.streamCreated == false {
		q.eventStore.CreateStream(ctx, q.name)
		q.streamCreated = true
	}

	return q.eventStore.AppendTo(ctx, q.name, []DomainEvent{event})
}

// LinkTo append the event to a given EventStream
func (q *Projector) LinkTo(ctx context.Context, streamName string, event DomainEvent) error {
	if ok, _ := q.eventStore.HasStream(ctx, streamName); ok == false {
		q.eventStore.CreateStream(ctx, streamName)
	}

	return q.eventStore.AppendTo(ctx, streamName, []DomainEvent{event})
}

// Delete the Projection from the Projections table / collection and if deleteEmittedEvents is true
// Also if exists the related Emit-EventStream
func (q *Projector) Delete(ctx context.Context, deleteEmittedEvents bool) error {
	err := q.manager.DeleteProjection(ctx, q.name)
	if err != nil {
		return err
	}

	if deleteEmittedEvents {
		err = q.eventStore.DeleteStream(ctx, q.name)
		if err != nil {
			return err
		}
	}

	return nil
}

// Reset the Projection state and EventStream positions
func (q *Projector) Reset(ctx context.Context) error {
	q.streamPositions = map[string]int{}
	q.state = struct{}{}

	if q.initHandler != nil {
		q.state = q.initHandler()
	}

	return q.manager.ResetProjection(ctx, q.name, q.state)
}

// Stop the Projection and persist the current state and EventStream positions
func (q *Projector) Stop(ctx context.Context) error {
	err := q.manager.PersistProjection(ctx, q.name, q.state, q.streamPositions)
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

// Run the Projection
func (q *Projector) Run(ctx context.Context, keepRunning bool) error {
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
		err = q.Delete(ctx, false)
		if err != nil {
			return err
		}
	case StatusDeletingIinclEmittedEevents:
		err = q.Delete(ctx, false)
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
			err = q.manager.PersistProjection(ctx, q.name, q.state, q.streamPositions)
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

// State returns the current Projection State
func (q Projector) State() interface{} {
	return q.state
}

// Name of the Projection
func (q Projector) Name() string {
	return q.name
}

// Status of the Projection
func (q Projector) Status() Status {
	return q.status
}

func (q *Projector) startAgain(ctx context.Context) error {
	q.isStopped = false

	err := q.manager.UpdateProjectionStatus(ctx, q.name, StatusRunning)
	if err != nil {
		return err
	}

	q.status = StatusRunning

	return nil
}

func (q *Projector) load(ctx context.Context) error {
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

func (q *Projector) persistAndFetchRemoteStatusWhenBlockSizeThresholdReached(ctx context.Context) error {
	if q.eventCounter != q.persistBlockSize {
		return nil
	}

	err := q.manager.PersistProjection(ctx, q.name, q.state, q.streamPositions)
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

func (q *Projector) prepareStreamPosition(ctx context.Context) error {
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

func (q *Projector) fetchRemoteStatus(ctx context.Context) Status {
	status, err := q.manager.FetchProjectionStatus(ctx, q.name)
	if err != nil {
		return StatusRunning
	}

	return status
}

func (q *Projector) retreiveEventsFromStream(ctx context.Context) (DomainEventIterator, error) {
	streams := []LoadStreamParameter{}

	for stream, position := range q.streamPositions {
		streams = append(streams, LoadStreamParameter{StreamName: stream, FromNumber: position + 1, Matcher: q.metadataMatchers[stream]})
	}

	return q.eventStore.MergeAndLoad(ctx, 0, streams...)
}

func (q *Projector) handleStream(ctx context.Context, events DomainEventIterator) error {
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

// NewProjector create a new Projector to configure and run a new projection
// Define your prefered persistence storage with the ProjectionManager (at this time only Postgres is supported :-D)
func NewProjector(name string, eventStore *EventStore, manager ProjectionManager) Projector {
	return Projector{
		name:             name,
		state:            nil,
		status:           StatusIdle,
		eventStore:       eventStore,
		manager:          manager,
		initHandler:      nil,
		handler:          nil,
		handlers:         map[string]func(state interface{}, event DomainEvent) interface{}{},
		metadataMatchers: map[string]MetadataMatcher{},
		streamPositions:  map[string]int{},
		isStopped:        false,
		streamCreated:    false,
		eventCounter:     0,
		persistBlockSize: 1000,
	}
}

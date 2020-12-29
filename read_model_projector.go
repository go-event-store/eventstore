package eventstore

import (
	"context"
	"errors"
	"sync"
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
	handler          EventHandler
	handlers         map[string]EventHandler
	metadataMatchers map[string]MetadataMatcher
	streamPositions  map[string]int
	persistBlockSize int
	eventCounter     int
	running          bool
	err              error
	wg               *sync.WaitGroup
	mx               *sync.RWMutex
	stopChan         chan bool

	query struct {
		all     bool
		streams []string
	}
}

// Init the state, define the type and/or prefill it with data
func (q *ReadModelProjector) Init(handler func() interface{}) *ReadModelProjector {
	if q.initHandler != nil {
		q.err = ProjectorAlreadyInitialized{}
	}

	q.initHandler = handler
	q.state = handler()

	return q
}

// FromAll read events from all existing EventStreams
func (q *ReadModelProjector) FromAll() *ReadModelProjector {
	if q.query.all || len(q.query.streams) > 0 {
		q.err = ProjectorFromWasAlreadyCalled{}
	}

	q.query.all = true

	return q
}

// FromStream read events from a single EventStream
func (q *ReadModelProjector) FromStream(streamName string, matcher MetadataMatcher) *ReadModelProjector {
	if q.query.all || len(q.query.streams) > 0 {
		q.err = ProjectorFromWasAlreadyCalled{}
	}

	q.query.streams = append(q.query.streams, streamName)
	q.metadataMatchers[streamName] = matcher

	return q
}

// FromStreams read events from multiple EventStreams
func (q *ReadModelProjector) FromStreams(streams ...StreamProjection) *ReadModelProjector {
	if q.query.all || len(q.query.streams) > 0 {
		q.err = ProjectorFromWasAlreadyCalled{}
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
		q.err = ProjectorHandlerAlreadyDefined{}
	}

	q.handlers = handlers

	return q
}

// WhenAny defines a single handler for all possible Events
func (q *ReadModelProjector) WhenAny(handler EventHandler) *ReadModelProjector {
	if q.handler != nil || len(q.handlers) != 0 {
		q.err = ProjectorHandlerAlreadyDefined{}
	}

	q.handler = handler

	return q
}

// Delete the ReadModelProjection from the Projections table / collection and if deleteProjection is true
// it also runs the Delete Method of your ReadModel
func (q *ReadModelProjector) Delete(ctx context.Context) error {
	if q.running {
		return errors.New("Could not be deleted while running")
	}

	err := q.manager.DeleteProjection(ctx, q.name)
	if err != nil {
		return err
	}

	return q.ReadModel.Delete(ctx)
}

// Reset the ReadModelProjection state and EventStream positions
// Run also the Reset Method of your ReadModel
func (q *ReadModelProjector) Reset(ctx context.Context) error {
	if q.running {
		return errors.New("Could not be resetted while running")
	}
	if q.initHandler == nil {
		return ProjectorHasNoInitCallback{}
	}
	err := q.ReadModel.Reset(ctx)
	if err != nil {
		return err
	}

	q.streamPositions = map[string]int{}
	q.state = q.initHandler()
	q.err = nil

	return q.manager.ResetProjection(ctx, q.name, q.state)
}

// Stop the ReadModelProjection and persist the current state and EventStream positions
func (q *ReadModelProjector) Stop(ctx context.Context) error {
	q.mx.Lock()
	q.status = StatusStopping
	q.mx.Unlock()

	if q.running {
		q.stopChan <- true
	}

	return q.manager.UpdateProjectionStatus(ctx, q.name, StatusIdle)
}

// Run the ReadModelProjection
func (q *ReadModelProjector) Run(ctx context.Context, keepRunning bool) error {
	if q.err != nil {
		return q.err
	}

	if q.handler == nil && len(q.handlers) == 0 {
		return ProjectorHasNoHandler{}
	}

	if q.state == nil {
		return ProjectorStateNotInitialised{}
	}

	var err error

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

	breakChan := make(chan bool, 1)
	persistChan := make(chan bool)
	errorChan := make(chan error)

	q.mx.Lock()
	q.running = true
	q.status = StatusRunning
	q.mx.Unlock()

	defer func() {
		q.running = false
		q.status = StatusIdle
		close(breakChan)
		close(errorChan)
		close(q.stopChan)

		q.stopChan = make(chan bool, 1)
	}()

	q.wg.Add(2)

	go q.processEvents(ctx, breakChan, persistChan, errorChan, keepRunning)
	go q.persist(ctx, persistChan, errorChan)

	select {
	case err := <-errorChan:
		q.wg.Wait()
		return err
	case <-q.stopChan:
		breakChan <- true
		q.wg.Wait()
		return q.err
	}
}

func (q *ReadModelProjector) processEvents(
	ctx context.Context,
	breakChan <-chan bool,
	persistChan chan<- bool,
	errorChan chan<- error,
	keepRunning bool,
) {
	defer func() {
		persistChan <- true
		close(persistChan)
		q.wg.Done()
	}()

	var counter int
	ticker := time.NewTicker(200 * time.Millisecond)

	for {
		events, err := q.retreiveEventsFromStream(ctx)
		if err != nil {
			errorChan <- err
			return
		}
		for events.Next() {
			event, err := events.Current()
			if err != nil {
				errorChan <- err
				return
			}

			err = q.handleStream(*event)
			if err != nil {
				errorChan <- err
				return
			}

			if q.Status() == StatusStopping {
				persistChan <- true
				return
			}

			counter++

			if counter == q.persistBlockSize {
				persistChan <- true
				counter = 0
			}
		}

		persistChan <- true
		counter = 0

		if !keepRunning {
			q.stopChan <- true
			return
		}

		select {
		case <-ticker.C:
		case <-breakChan:
			return
		}
	}
}

func (q *ReadModelProjector) persist(ctx context.Context, persistChan <-chan bool, errorChan chan<- error) {
	defer q.wg.Done()

	for range persistChan {
		err := q.persistReadModel(ctx)
		if err != nil {
			errorChan <- err
			return
		}
	}
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
	q.mx.RLock()
	defer q.mx.RUnlock()

	return q.status
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

func (q *ReadModelProjector) persistReadModel(ctx context.Context) error {
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

func (q *ReadModelProjector) handleStream(event DomainEvent) error {
	var err error

	q.streamPositions[event.Metadata()["stream"].(string)] = event.Number()

	if q.handler != nil {
		q.state, err = q.handler(q.state, event)
	}

	if handler, ok := q.handlers[event.Name()]; ok {
		q.state, err = handler(q.state, event)
	}

	return err
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
		handlers:         map[string]EventHandler{},
		metadataMatchers: map[string]MetadataMatcher{},
		streamPositions:  map[string]int{},
		running:          false,
		eventCounter:     0,
		persistBlockSize: 1000,
		wg:               new(sync.WaitGroup),
		mx:               new(sync.RWMutex),
		stopChan:         make(chan bool, 1),
	}
}

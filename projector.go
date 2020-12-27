package eventstore

import (
	"context"
	"errors"
	"sync"
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
	handler          EventHandler
	handlers         map[string]EventHandler
	metadataMatchers map[string]MetadataMatcher
	streamPositions  map[string]int
	persistBlockSize int
	eventCounter     int
	streamCreated    bool
	running          bool
	err              error
	wg               *sync.WaitGroup
	stateMutex       *sync.Mutex
	positionMutex    *sync.Mutex

	statusChan chan Status

	query struct {
		all     bool
		streams []string
	}
}

// Init the state, define the type and/or prefill it with data
func (q *Projector) Init(handler func() interface{}) *Projector {
	if q.initHandler != nil {
		q.err = ProjectorAlreadyInitialized{}
	}

	q.initHandler = handler
	q.state = handler()

	return q
}

// FromAll read events from all existing EventStreams
func (q *Projector) FromAll() *Projector {
	if q.query.all || len(q.query.streams) > 0 {
		q.err = ProjectorFromWasAlreadyCalled{}
	}

	q.query.all = true

	return q
}

// FromStream read events from a single EventStream
func (q *Projector) FromStream(streamName string, matcher MetadataMatcher) *Projector {
	if q.query.all || len(q.query.streams) > 0 {
		q.err = ProjectorFromWasAlreadyCalled{}
	}

	q.query.streams = append(q.query.streams, streamName)
	q.metadataMatchers[streamName] = matcher

	return q
}

// FromStreams read events from multiple EventStreams
func (q *Projector) FromStreams(streams ...StreamProjection) *Projector {
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
func (q *Projector) When(handlers map[string]EventHandler) *Projector {
	if q.handler != nil || len(q.handlers) != 0 {
		q.err = ProjectorHandlerAlreadyDefined{}
	}

	q.handlers = handlers

	return q
}

// WhenAny defines a single handler for all possible Events
func (q *Projector) WhenAny(handler EventHandler) *Projector {
	if q.handler != nil || len(q.handlers) != 0 {
		q.err = ProjectorHandlerAlreadyDefined{}
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
	if q.running {
		return errors.New("Could not be deleted while running")
	}

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
	q.stateMutex.Lock()
	q.state = struct{}{}
	q.stateMutex.Unlock()

	q.stateMutex.Lock()
	q.streamPositions = map[string]int{}
	q.stateMutex.Unlock()

	if q.initHandler != nil {
		q.stateMutex.Lock()
		q.state = q.initHandler()
		q.stateMutex.Unlock()
	}

	if q.running {
		q.statusChan <- StatusResetting
	}

	return q.manager.ResetProjection(ctx, q.name, q.state)
}

// Stop the Projection and persist the current state and EventStream positions
func (q *Projector) Stop(ctx context.Context) error {
	err := q.manager.UpdateProjectionStatus(ctx, q.name, StatusIdle)
	if err != nil {
		return err
	}

	if q.running {
		q.statusChan <- StatusStopping
	}

	return nil
}

// Run the Projection
func (q *Projector) Run(ctx context.Context, keepRunning bool) error {
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

	if ok, err := q.manager.ProjectionExists(ctx, q.name); !ok {
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

	stopChan := make(chan bool)
	errorChan := make(chan error)
	eventChan := make(chan DomainEvent)
	persistChan := make(chan int)

	q.running = true

	defer func() {
		close(stopChan)
		close(errorChan)
	}()

	q.wg.Add(3)

	go q.fetchEvents(ctx, stopChan, eventChan, errorChan)
	go q.handleEvents(persistChan, eventChan, errorChan)
	go q.persist(ctx, persistChan, errorChan)

	go func() {
		if !keepRunning {
			err := q.Stop(ctx)
			if err != nil {
				q.err = err
			}
		}
	}()

	select {
	case err := <-errorChan:
		stopChan <- true

		q.wg.Wait()
		return err
	case status := <-q.statusChan:
		q.status = status

		stopChan <- true
		q.running = false
		q.wg.Wait()
		return q.err
	}
}

func (q *Projector) fetchEvents(ctx context.Context, stopChan <-chan bool, eventChan chan<- DomainEvent, errorChan chan<- error) {
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

			eventChan <- *event
		}

		select {
		case <-ticker.C:
		case <-stopChan:
			close(eventChan)
			q.wg.Done()
			return
		}
	}
}

func (q *Projector) handleEvents(persistChan chan<- int, eventChan <-chan DomainEvent, errorChan chan<- error) {
	defer func() {
		persistChan <- 1
		close(persistChan)
		q.wg.Done()
	}()
	var counter int
	var err error

	for event := range eventChan {
		err = q.handleStream(event)
		if err != nil {
			errorChan <- err
			return
		}

		counter++

		if counter == q.persistBlockSize {
			persistChan <- 1
		}
	}
}

func (q *Projector) persist(ctx context.Context, persistChan <-chan int, errorChan chan<- error) {
	defer q.wg.Done()

	for range persistChan {
		err := q.manager.PersistProjection(ctx, q.name, q.state, q.streamPositions)
		if err != nil {
			errorChan <- err
			return
		}
	}
}

// State returns the current Projection State
func (q *Projector) State() interface{} {
	return q.state
}

// Name of the Projection
func (q *Projector) Name() string {
	return q.name
}

// Status of the Projection
func (q *Projector) Status() Status {
	return q.status
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

func (q *Projector) handleStream(event DomainEvent) error {
	var err error
	var state interface{}

	q.positionMutex.Lock()
	q.streamPositions[event.Metadata()["stream"].(string)] = event.Number()
	q.positionMutex.Unlock()

	if q.handler != nil {
		state, err = q.handler(q.state, event)

		q.stateMutex.Lock()
		q.state = state
		q.stateMutex.Unlock()
	}

	if handler, ok := q.handlers[event.Name()]; ok {
		state, err = handler(q.state, event)

		q.stateMutex.Lock()
		q.state = state
		q.stateMutex.Unlock()
	}

	return err
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
		handlers:         map[string]EventHandler{},
		metadataMatchers: map[string]MetadataMatcher{},
		streamPositions:  map[string]int{},
		running:          false,
		streamCreated:    false,
		eventCounter:     0,
		persistBlockSize: 1000,
		wg:               new(sync.WaitGroup),
		stateMutex:       new(sync.Mutex),
		positionMutex:    new(sync.Mutex),

		statusChan: make(chan Status),
	}
}

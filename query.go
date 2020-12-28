package eventstore

import (
	"context"
	"errors"
	"sync"
)

// Query custom informations from your EventStream
// Queries are not persisted, they provide the latest state after running
type Query struct {
	state            interface{}
	status           Status
	eventStore       *EventStore
	initHandler      func() interface{}
	handler          EventHandler
	handlers         map[string]EventHandler
	metadataMatchers map[string]MetadataMatcher
	streamPositions  map[string]int
	running          bool
	err              error
	wg               *sync.WaitGroup

	query struct {
		all     bool
		streams []string
	}
}

// Init the state, define the type and/or prefill it with data
func (q *Query) Init(handler func() interface{}) *Query {
	if q.initHandler != nil {
		q.err = ProjectorAlreadyInitialized{}
	}

	q.initHandler = handler
	q.state = handler()

	return q
}

// FromAll read events from all existing EventStreams
func (q *Query) FromAll() *Query {
	if q.query.all || len(q.query.streams) > 0 {
		q.err = ProjectorFromWasAlreadyCalled{}
	}

	q.query.all = true

	return q
}

// FromStream read events from a single EventStream
func (q *Query) FromStream(streamName string, matcher MetadataMatcher) *Query {
	if q.query.all || len(q.query.streams) > 0 {
		q.err = ProjectorFromWasAlreadyCalled{}
	}

	q.query.streams = append(q.query.streams, streamName)
	q.metadataMatchers[streamName] = matcher

	return q
}

// FromStreams read events from multiple EventStreams
func (q *Query) FromStreams(streams ...StreamProjection) *Query {
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
func (q *Query) When(handlers map[string]EventHandler) *Query {
	if q.handler != nil || len(q.handlers) != 0 {
		q.err = ProjectorHandlerAlreadyDefined{}
	}

	q.handlers = handlers

	return q
}

// WhenAny defines a single handler for all possible Events
func (q *Query) WhenAny(handler EventHandler) *Query {
	if q.handler != nil || len(q.handlers) != 0 {
		q.err = ProjectorHandlerAlreadyDefined{}
	}

	q.handler = handler

	return q
}

// Reset the query state and EventStream positions
func (q *Query) Reset() error {
	if q.running {
		return errors.New("Could not be resetted while running")
	}
	if q.initHandler == nil {
		return ProjectorHasNoInitCallback{}
	}

	q.streamPositions = map[string]int{}
	q.state = q.initHandler()
	q.err = nil

	return nil
}

// Stop the query
func (q *Query) Stop() {
	q.status = StatusStopping
}

// Run the Query
func (q *Query) Run(ctx context.Context) error {
	if q.err != nil {
		return q.err
	}

	if q.handler == nil && len(q.handlers) == 0 {
		return ProjectorHasNoHandler{}
	}

	if q.state == nil {
		return ProjectorStateNotInitialised{}
	}

	err := q.prepareStreamPosition(ctx)
	if err != nil {
		return err
	}

	errorChan := make(chan error)

	q.running = false
	q.status = StatusRunning

	defer func() {
		q.running = false
		q.status = StatusIdle
		close(errorChan)
	}()

	q.wg.Add(1)

	go q.processEvents(ctx, errorChan)

	select {
	case err := <-errorChan:
		q.wg.Wait()
		return err
	}
}

func (q *Query) processEvents(ctx context.Context, errorChan chan<- error) {
	defer q.wg.Done()

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

		if q.status == StatusStopping {
			errorChan <- nil
			return
		}
	}

	errorChan <- nil
}

func (q *Query) handleStream(event DomainEvent) error {
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

// State returns the current query State
func (q Query) State() interface{} {
	return q.state
}

// Status returns the current query Status
func (q Query) Status() interface{} {
	return q.status
}

func (q *Query) prepareStreamPosition(ctx context.Context) error {
	streams := []string{}
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

func (q *Query) retreiveEventsFromStream(ctx context.Context) (DomainEventIterator, error) {
	streams := []LoadStreamParameter{}

	for stream, position := range q.streamPositions {
		streams = append(streams, LoadStreamParameter{StreamName: stream, FromNumber: position + 1, Matcher: q.metadataMatchers[stream]})
	}

	return q.eventStore.MergeAndLoad(ctx, 0, streams...)
}

// NewQuery for the given EventStore
func NewQuery(eventStore *EventStore) Query {
	return Query{
		state:            nil,
		status:           StatusIdle,
		eventStore:       eventStore,
		initHandler:      nil,
		handler:          nil,
		handlers:         map[string]EventHandler{},
		metadataMatchers: map[string]MetadataMatcher{},
		streamPositions:  map[string]int{},
		running:          false,
		wg:               new(sync.WaitGroup),
	}
}

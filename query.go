package eventstore

import (
	"context"
)

// Query custom informations from your EventStream
// Queries are not persisted, they provide the latest state after running
type Query struct {
	state            interface{}
	Status           Status
	eventStore       *EventStore
	initHandler      func() interface{}
	handler          EventHandler
	handlers         map[string]EventHandler
	metadataMatchers map[string]MetadataMatcher
	streamPositions  map[string]int
	isStopped        bool
	err              error

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
func (q *Query) Reset() {
	q.streamPositions = map[string]int{}
	q.state = struct{}{}
	q.err = nil

	if q.initHandler != nil {
		q.state = q.initHandler()
	}
}

// Stop the query
func (q *Query) Stop() {
	q.Status = StatusIdle
	q.isStopped = true
}

// Run the Query
func (q *Query) Run(ctx context.Context) error {
	if q.err != nil {
		return q.err
	}

	if q.handler == nil && len(q.handlers) == 0 {
		q.err = ProjectorHasNoHandler{}
	}

	if q.state == nil {
		q.err = ProjectorStateNotInitialised{}
	}

	q.isStopped = false
	q.Status = StatusRunning

	err := q.prepareStreamPosition(ctx)
	if err != nil {
		return err
	}

	events, err := q.retreiveEventsFromStream(ctx)
	if err != nil {
		return err
	}

	q.state, q.err = q.handleEvents(q.state, events)

	return q.err
}

// State returns the current query State
func (q Query) State() interface{} {
	return q.state
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

func (q *Query) handleEvents(state interface{}, events DomainEventIterator) (interface{}, error) {
	var err error
	for events.Next() {
		event, err := events.Current()
		if err != nil {
			return state, err
		}

		if q.handler != nil {
			state, err = q.handler(state, *event)
		}

		if handler, ok := q.handlers[event.Name()]; ok {
			state, err = handler(state, *event)
		}

		if q.isStopped {
			return state, err
		}
	}

	return state, err
}

// NewQuery for the given EventStore
func NewQuery(eventStore *EventStore) Query {
	return Query{
		state:            nil,
		Status:           StatusIdle,
		eventStore:       eventStore,
		initHandler:      nil,
		handler:          nil,
		handlers:         map[string]EventHandler{},
		metadataMatchers: map[string]MetadataMatcher{},
		streamPositions:  map[string]int{},
		isStopped:        false,
	}
}

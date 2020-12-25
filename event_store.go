package eventstore

import (
	"context"
	"fmt"
)

type LoadStreamParameter struct {
	StreamName string
	FromNumber int
	Matcher    MetadataMatcher
}

// PersistenceStrategy defines an Interface needed for an underlying PersistentStorage
// Current implementations are Postgres and InMemory
type PersistenceStrategy interface {
	// FetchAllStreamNames returns a list of all existing EventStreams
	FetchAllStreamNames(ctx context.Context) ([]string, error)
	// CreateEventStreamsTable creates a DB Table / Collection to manage all existing EventStreams
	CreateEventStreamsTable(context.Context) error
	// CreateProjectionsTable creates a DB Table / Collection to manage all existing Projections
	CreateProjectionsTable(context.Context) error
	// AddStreamToStreamsTable adds a new EventStream to the EventStreams Table / Collection
	AddStreamToStreamsTable(ctx context.Context, streamName string) error
	// RemoveStreamFromStreamsTable removes a EventStream from the EventStreams Table / Collection
	RemoveStreamFromStreamsTable(ctx context.Context, streamName string) error
	// DeleteStream deletes the EventStream from the EventStreams Table / Collection and deletes the related EventStream Table / Collection
	DeleteStream(ctx context.Context, streamName string) error
	// CreateSchema creates a new EventStream Table / Collection which is used to persist all related Events
	CreateSchema(ctx context.Context, streamName string) error
	// DropSchema removes a EventStream Table / Collection with all included Events
	DropSchema(ctx context.Context, streamName string) error
	// HasStream return if a EventStream with the given name exists
	HasStream(ctx context.Context, streamName string) (bool, error)
	// AppendTo appends multiple events to the given EventStream
	AppendTo(ctx context.Context, streamName string, events []DomainEvent) error
	// Load Events from the given EventStream from the given Number, restrictable by a count (Limit) and custom filter (MetadataMatcher)
	Load(ctx context.Context, streamName string, fromNumber int, count int, matcher MetadataMatcher) (DomainEventIterator, error)
	// MergeAndLoad can load Events from multiple Stream merged and sorted by the historical order
	// The Result could also be restriced by count and MetadataMatcher
	MergeAndLoad(ctx context.Context, count int, streams ...LoadStreamParameter) (DomainEventIterator, error)
}

// EventStore represent the connection to our EventStream with the selected PersistenceStrategy
type EventStore struct {
	strategy   PersistenceStrategy
	middleware map[DomainEventAction][]DomainEventMiddleware
}

// Install creates the EventStreams and Projections Table if not Exists
func (es *EventStore) Install(ctx context.Context) error {
	err := es.strategy.CreateEventStreamsTable(ctx)
	if err != nil {
		return fmt.Errorf("Unable to create EventStreams Table: %s", err.Error())
	}
	err = es.strategy.CreateProjectionsTable(ctx)
	if err != nil {
		return fmt.Errorf("Unable to create Projections Table: %s", err.Error())
	}
	return nil
}

// CreateStream creates a new EventStream
func (es *EventStore) CreateStream(ctx context.Context, streamName string) error {
	err := es.strategy.AddStreamToStreamsTable(ctx, streamName)
	if err != nil {
		return err
	}
	err = es.strategy.CreateSchema(ctx, streamName)
	if err != nil {
		err = es.strategy.RemoveStreamFromStreamsTable(ctx, streamName)
	}
	if err != nil {
		return fmt.Errorf("Failed to create Stream Schema: %s", err.Error())
	}

	return nil
}

// AppendTo appends a list of events to the given EventStream
func (es *EventStore) AppendTo(ctx context.Context, streamName string, events []DomainEvent) error {
	if len(events) == 0 {
		return nil
	}

	updatedEvents, err := es.executeMiddleware(ctx, PreAppend, events)
	if err != nil {
		return err
	}

	err = es.strategy.AppendTo(ctx, streamName, updatedEvents)
	if err != nil {
		return fmt.Errorf("Failed Append Events: %s", err.Error())
	}

	updatedEvents, err = es.executeMiddleware(ctx, Appended, events)
	if err != nil {
		return err
	}

	return nil
}

// Load Events from the given EventStream from the given Number, restrictable by a count (Limit) and custom filter (MetadataMatcher)
func (es *EventStore) Load(ctx context.Context, streamName string, fromNumber, count int, matcher MetadataMatcher) (DomainEventIterator, error) {
	it, err := es.strategy.Load(ctx, streamName, fromNumber, count, matcher)
	if it != nil && len(es.middleware[Loaded]) > 0 {
		return NewMiddlewareIterator(ctx, it, es.middleware[Loaded]), err
	}

	return it, err
}

// MergeAndLoad can load Events from multiple Stream merged and sorted by the historical order
// The Result could also be restriced by count and MetadataMatcher
func (es *EventStore) MergeAndLoad(ctx context.Context, count int, streams ...LoadStreamParameter) (DomainEventIterator, error) {
	it, err := es.strategy.MergeAndLoad(ctx, count, streams...)
	if it != nil && len(es.middleware[Loaded]) > 0 {
		return NewMiddlewareIterator(ctx, it, es.middleware[Loaded]), err
	}

	return it, err
}

// DeleteStream deletes an existing EventStream
func (es *EventStore) DeleteStream(ctx context.Context, streamName string) error {
	return es.strategy.DeleteStream(ctx, streamName)
}

// HasStream return if a EventStream with the given name exists
func (es *EventStore) HasStream(ctx context.Context, streamName string) (bool, error) {
	return es.strategy.HasStream(ctx, streamName)
}

// FetchAllStreamNames returns a list of all existing EventStreams
func (es *EventStore) FetchAllStreamNames(ctx context.Context) ([]string, error) {
	return es.strategy.FetchAllStreamNames(ctx)
}

// Append a middleware to one of the existing Actions PreAppend, Appended, Loaded
func (es *EventStore) AppendMiddleware(action DomainEventAction, middleware DomainEventMiddleware) {
	es.middleware[action] = append(es.middleware[action], middleware)
}

func (es *EventStore) executeMiddleware(ctx context.Context, action DomainEventAction, events []DomainEvent) ([]DomainEvent, error) {
	var err error

	if len(es.middleware[action]) > 0 {
		updatedEvents := make([]DomainEvent, 0, len(events))

		for _, event := range events {
			for _, middleware := range es.middleware[action] {
				event, err = middleware(ctx, event)
				if err != nil {
					return []DomainEvent{}, fmt.Errorf("Failed to process %s Middleware: %s", action, err.Error())
				}
			}

			updatedEvents = append(updatedEvents, event)
		}

		return updatedEvents, nil
	}

	return events, nil
}

// NewEventStore creates a BasicEventStore with the selected PersistenceStrategy
func NewEventStore(strategy PersistenceStrategy) *EventStore {
	return &EventStore{
		strategy: strategy,
		middleware: map[DomainEventAction][]DomainEventMiddleware{
			PreAppend: make([]DomainEventMiddleware, 0),
			Appended:  make([]DomainEventMiddleware, 0),
			Loaded:    make([]DomainEventMiddleware, 0),
		},
	}
}

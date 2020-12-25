package eventstore

import (
	"reflect"
	"time"

	uuid "github.com/satori/go.uuid"
)

// Aggregate define the Basic Methods required by any Implementation
type Aggregate interface {
	PopEvents() []DomainEvent
	AggregateID() uuid.UUID
	FromHistory(events DomainEventIterator) error
}

// BaseAggregate provide basic functionallity to simplify Aggregate handling
// Like creation, recreation and loading of Aggregates
type BaseAggregate struct {
	aggregateID    uuid.UUID
	version        int
	source         interface{}
	recordedEvents []DomainEvent
	handlersCache  HandlersCache
}

// AggregateID is a unique identifier for an Aggregate instance
// All Aggregate Events are grouped by this UUID
func (a BaseAggregate) AggregateID() uuid.UUID {
	return a.aggregateID
}

// Version returns the current Version of the Aggregate
func (a BaseAggregate) Version() int {
	return a.version
}

// RecordThat add a new Event to the EventStream of an Aggregate instance
// {event} represent a basic struct with the Event payload
// {metadata} is an map with additional informations and can be used to filter the EventStream in Projections or Queries
func (a *BaseAggregate) RecordThat(event interface{}, metadata map[string]interface{}) {
	a.Apply(NewDomainEvent(a.aggregateID, event, metadata, time.Now()))
}

// Apply add a already wrapped Event in an DomainEvent to the Stack
// With this Method you have more control of the wrapped DomainEvent
func (a *BaseAggregate) Apply(event DomainEvent) {
	a.version++

	pEvent := event.
		WithVersion(a.version).
		WithAggregateType(reflect.TypeOf(a.source).Elem().Name())

	a.recordedEvents = append(a.recordedEvents, pEvent)
	a.CallEventHandler(event.Payload(), event.Metadata())
}

// FromHistory recreate the latest state of an existing Aggregate by its recorded Events
func (a *BaseAggregate) FromHistory(events DomainEventIterator) error {
	for events.Next() {
		event, err := events.Current()
		if err != nil {
			return err
		}

		a.version = event.Version()
		a.aggregateID = event.AggregateID()
		a.CallEventHandler(event.Payload(), event.Metadata())
	}

	return nil
}

// PopEvents return and clear all new and not persisted events of the stack
func (a *BaseAggregate) PopEvents() []DomainEvent {
	events := a.recordedEvents

	a.recordedEvents = []DomainEvent{}

	return events
}

// CallEventHandler is an internal Method who calls the related Handler Method of the Aggregate after a new Event was recorded
// The Method has to have the Schema When{EventName}(event EventStruct) -> an example: func (f *FooAggregate) WhenFooEvent(e FooEvent, metadata map[string]interface{})
func (a *BaseAggregate) CallEventHandler(event interface{}, metadata map[string]interface{}) {
	eventType := reflect.TypeOf(event)

	if handler, ok := a.handlersCache[eventType]; ok {
		handler(a.source, event, metadata)
	}
}

// NewAggregate is a Constructor Function to create a new BaseAggregate
func NewAggregate(source interface{}) BaseAggregate {
	return BaseAggregate{
		aggregateID:    uuid.NewV4(),
		version:        0,
		source:         source,
		recordedEvents: []DomainEvent{},
		handlersCache:  createHandlersCache(source),
	}
}

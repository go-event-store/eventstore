package eventstore

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	uuid "github.com/satori/go.uuid"
)

// DomainEvent represent a Wrapper for DomainEvents to add meta informations
// It is used to simplify the handling of adding and recreate new DomainEvents
type DomainEvent struct {
	number    int
	uuid      uuid.UUID
	name      string
	payload   interface{}
	metadata  map[string]interface{}
	createdAt time.Time
}

// Number of the event in the complete EventStream (over multiple aggregates)
func (e DomainEvent) Number() int {
	return e.number
}

// UUID to identify a single Event
func (e DomainEvent) UUID() uuid.UUID {
	return e.uuid
}

// Name of the DomainEvent
func (e DomainEvent) Name() string {
	return e.name
}

// Payload is the original recorded DomainEventStruct
// It is set and modified as Integer
// Postgres as persistence layout read the value as float64 to it has to be converted in this scenarios
func (e DomainEvent) Version() int {
	switch v := e.metadata["_aggregate_version"].(type) {
	case float64:
		return int(v)
	case int:
		return v
	default:
		ver, err := strconv.Atoi(fmt.Sprintf("%v", v))
		if err != nil {
			panic(err)
		}

		return ver
	}
}

// Payload is the original recorded DomainEventStruct
func (e DomainEvent) Payload() interface{} {
	return e.payload
}

// Metadata of the Event like the related AggregateID
func (e DomainEvent) Metadata() map[string]interface{} {
	return e.metadata
}

// SingleMetadata from the Metadata Map
func (e DomainEvent) SingleMetadata(key string) (interface{}, bool) {
	v, ok := e.metadata[key]

	return v, ok
}

// CreatedAt is the creation DateTime of the Event
func (e DomainEvent) CreatedAt() time.Time {
	return e.createdAt
}

// AggregateType of the Event
func (e DomainEvent) AggregateType() string {
	return e.metadata["_aggregate_type"].(string)
}

// AggregateID of the related Aggregate
func (e DomainEvent) AggregateID() uuid.UUID {
	return uuid.FromStringOrNil(e.metadata["_aggregate_id"].(string))
}

// WithVersion create a copy of the event with the given Version
func (e DomainEvent) WithVersion(v int) DomainEvent {
	metadata := CopyMap(e.metadata)
	metadata["_aggregate_version"] = v
	e.metadata = metadata

	return e
}

// WithAggregateType create a copy of the event with the given AggregateType
func (e DomainEvent) WithAggregateType(aType string) DomainEvent {
	metadata := CopyMap(e.metadata)
	metadata["_aggregate_type"] = aType
	e.metadata = metadata

	return e
}

// WithAddedMetadata create a copy of the event with the given additional Metadata
func (e DomainEvent) WithAddedMetadata(name string, value interface{}) DomainEvent {
	switch value.(type) {
	case string, int, float64:
		metadata := CopyMap(e.metadata)
		metadata[name] = value
		e.metadata = metadata
	default:
		panic("only int, string, float are supported as additional metadata for now")
	}

	return e
}

// WithUUID create a copy of the event with the given EventID
// Used to recreate a existing event from the underlying persistence storage
func (e DomainEvent) WithUUID(uuid uuid.UUID) DomainEvent {
	e.uuid = uuid

	return e
}

// WithNumber create a copy of the event with the given number
// Is used to update the Number after adding to the EventStream or to recreate an existing Event
func (e DomainEvent) WithNumber(number int) DomainEvent {
	e.number = number

	return e
}

// NewDomainEvent creates a new Event and set default vaues like Metadata
// Creates the EventID
func NewDomainEvent(aggregateID uuid.UUID, payload interface{}, metadata map[string]interface{}, createdAt time.Time) DomainEvent {
	e := DomainEvent{}
	e.name = reflect.TypeOf(payload).Name()
	e.uuid = uuid.NewV4()
	e.payload = payload
	e.number = 1

	if metadata != nil {
		e.metadata = metadata
	} else {
		e.metadata = make(map[string]interface{})
	}

	if _, ok := e.metadata["_aggregate_id"]; ok == false {
		e.metadata["_aggregate_id"] = aggregateID.String()
	}

	if _, ok := e.metadata["_aggregate_version"]; ok == false {
		e.metadata["_aggregate_version"] = 1
	}

	if _, ok := e.metadata["_aggregate_type"]; ok == false {
		e.metadata["_aggregate_type"] = ""
	}

	e.createdAt = createdAt

	return e
}

// CopyMap is a helper function to copy a map to create immutable events with all methods
func CopyMap(m map[string]interface{}) map[string]interface{} {
	cp := make(map[string]interface{})
	for k, v := range m {
		cp[k] = v
	}

	return cp
}

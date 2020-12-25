package eventstore_test

import (
	"reflect"
	"testing"
	"time"

	eventstore "github.com/fjogeleit/go-event-store"
	uuid "github.com/satori/go.uuid"
)

func Test_CreateEvents(t *testing.T) {
	t.Run("New Events have default Metadata", func(t *testing.T) {
		event := eventstore.NewDomainEvent(uuid.NewV4(), struct{}{}, map[string]interface{}{}, time.Now())

		if _, ok := event.Metadata()["_aggregate_id"]; ok == false {
			t.Error("Metadata should have an AggregateID")
		}

		if _, ok := event.Metadata()["_aggregate_version"]; ok == false {
			t.Error("Metadata should have a Aggregate Version")
		}

		if _, ok := event.Metadata()["_aggregate_type"]; ok == false {
			t.Error("Metadata should have a Aggregate Type")
		}
	})

	t.Run("Can access default metadata with helper methods", func(t *testing.T) {
		aggregateID := uuid.NewV4()

		event := eventstore.NewDomainEvent(aggregateID, struct{}{}, map[string]interface{}{}, time.Now())

		if event.AggregateID() != aggregateID && event.AggregateID() != event.Metadata()["_aggregate_id"] {
			t.Error("AggregateID should represent the given AggregateID from the Constructor Method")
		}

		if event.AggregateType() != event.Metadata()["_aggregate_type"] {
			t.Error("AggregateType should return the same value as the represented metadata value")
		}

		if event.Version() != event.Metadata()["_aggregate_version"] {
			t.Error("AggregateType should return the same value as the represented metadata value")
		}
	})

	t.Run("Can access event data with GetMethods", func(t *testing.T) {
		type FooEvent struct {
			Foo string
		}

		aggregateID := uuid.NewV4()
		createdAt := time.Now()
		payload := FooEvent{Foo: "Bar"}

		event := eventstore.NewDomainEvent(aggregateID, payload, map[string]interface{}{}, createdAt)

		if event.CreatedAt() != createdAt {
			t.Error("Should represent the given createdAt time")
		}

		if event.Name() != reflect.TypeOf(payload).Name() {
			t.Error("Should be the name of the payload struct")
		}

		if event.Number() != 1 {
			t.Error("Represent the order in the eventStream, default should be one")
		}

		if event.UUID() == uuid.Nil {
			t.Error("Unique Event Identifier, should not be empty")
		}

		if event.Payload().(FooEvent) != payload {
			t.Error("Payload represent the wrapped event data and should be equal to the constructor payload")
		}
	})

	t.Run("Can access event data with GetMethods", func(t *testing.T) {
		event := eventstore.NewDomainEvent(uuid.NewV4(), struct{}{}, map[string]interface{}{}, time.Now())

		eventWithVersion := event.WithVersion(2)

		if eventWithVersion.Version() != 2 || event.Version() == eventWithVersion.Version() {
			t.Error("WithVersion should create a copy of the source event with the given version")
		}

		eventWithNumber := event.WithNumber(2)

		if eventWithNumber.Number() != 2 || event.Number() == eventWithNumber.Number() {
			t.Error("WithNumber should create a copy of the source event with the given number")
		}

		eventWithAggregateType := event.WithAggregateType("FooAggregate")

		if eventWithAggregateType.AggregateType() != "FooAggregate" || event.AggregateType() == eventWithAggregateType.AggregateType() {
			t.Error("WithAggregateType should create a copy of the source event with the given AggregateType")
		}

		eventID := uuid.NewV4()

		eventWithUUID := event.WithUUID(eventID)

		if eventWithUUID.UUID() != eventID || event.UUID() == eventWithUUID.UUID() {
			t.Error("WithUUID should create a copy of the source event with the given UUID as Event Identifier")
		}
	})

	t.Run("Can add additional metadata", func(t *testing.T) {
		var event eventstore.DomainEvent

		event = eventstore.NewDomainEvent(uuid.NewV4(), struct{}{}, map[string]interface{}{}, time.Now())

		event = event.WithAddedMetadata("string", "value")
		event = event.WithAddedMetadata("integer", 5)
		event = event.WithAddedMetadata("float", float64(3.33))

		if v, _ := event.SingleMetadata("string"); v != "value" {
			t.Error("Metdata should include added string 'value'")
		}

		if v, _ := event.SingleMetadata("integer"); v != 5 {
			t.Error("Metdata should include added integer '5'")
		}

		if v, _ := event.SingleMetadata("float"); v != float64(3.33) {
			t.Error("Metdata should include added float64 '3.33'")
		}
	})
}

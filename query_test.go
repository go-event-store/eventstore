package eventstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	eventstore "github.com/go-event-store/eventstore"
	"github.com/go-event-store/eventstore/memory"
	uuid "github.com/satori/go.uuid"
)

func Test_Queries(t *testing.T) {
	type FooEvent struct {
		Foo string
	}

	type BarEvent struct {
		Bar string
	}

	beforeEach := func(ctx context.Context, aggregateID uuid.UUID) (eventstore.Query, *eventstore.EventStore) {
		es := eventstore.NewEventStore(memory.NewPersistenceStrategy())
		es.Install(ctx)
		es.CreateStream(ctx, "foo-stream")

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo1"}, map[string]interface{}{}, time.Now()),
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo2"}, map[string]interface{}{}, time.Now()).WithVersion(2),
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo3"}, map[string]interface{}{}, time.Now()).WithVersion(3),
			eventstore.NewDomainEvent(uuid.NewV4(), BarEvent{"Bar"}, map[string]interface{}{}, time.Now()),
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo4"}, map[string]interface{}{}, time.Now()).WithVersion(4),
		})

		query := eventstore.NewQuery(es)
		query.
			Init(func() interface{} {
				return []string{}
			}).
			When(map[string]eventstore.EventHandler{
				"FooEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					return append(state.([]string), event.Payload().(FooEvent).Foo), nil
				},
				"BarEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					return append(state.([]string), event.Payload().(BarEvent).Bar), nil
				},
			})

		return query, es
	}

	t.Run("Query all Events", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		query, _ := beforeEach(ctx, aggregateID)

		query.
			FromStream("foo-stream", nil).
			Run(ctx)

		state := query.State().([]string)

		if len(state) != 5 {
			t.Error("Query should return a list of all Event Payloads")
		}

		if state[0] != "Foo1" || state[1] != "Foo2" || state[2] != "Foo3" || state[3] != "Bar" || state[4] != "Foo4" {
			t.Error("Query should values in historical order")
		}
	})

	t.Run("Query Events by Aggregate", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		matcher := []eventstore.MetadataMatch{
			{
				Field:     "_aggregate_id",
				FieldType: eventstore.MetadataField,
				Operation: eventstore.EqualsOperator,
				Value:     aggregateID.String(),
			},
		}

		query, _ := beforeEach(ctx, aggregateID)
		query.
			FromStream("foo-stream", matcher).
			Run(ctx)

		state := query.State().([]string)

		if len(state) != 4 {
			t.Error("Query should return a list of all Event of the given AggregateID")
		}

		if state[0] != "Foo1" || state[1] != "Foo2" || state[2] != "Foo3" || state[3] != "Foo4" {
			t.Error("Query should values in historical order")
		}
	})

	t.Run("Query Events by Type and Version", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		matcher := []eventstore.MetadataMatch{
			{
				Field:     "event_name",
				FieldType: eventstore.MessagePropertyField,
				Operation: eventstore.EqualsOperator,
				Value:     "FooEvent",
			},
			{
				Field:     "_aggregate_version",
				FieldType: eventstore.MetadataField,
				Operation: eventstore.GreaterThanOperator,
				Value:     2,
			},
		}

		query, _ := beforeEach(ctx, aggregateID)
		query.
			FromStream("foo-stream", matcher).
			Run(ctx)

		state := query.State().([]string)

		if len(state) != 2 {
			t.Error("Query should return events of type FooEvent and a Version greather than two")
		}

		if state[0] != "Foo3" || state[1] != "Foo4" {
			t.Error("Query should values in historical order")
		}
	})

	t.Run("Reset Query", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		query, _ := beforeEach(ctx, aggregateID)

		query.
			FromStream("foo-stream", nil).
			Run(ctx)

		query.Reset()

		state := query.State().([]string)

		if len(state) != 0 {
			t.Error("State should be resetted")
		}
	})

	t.Run("Query all Events from all streams", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		es := eventstore.NewEventStore(memory.NewPersistenceStrategy())
		es.Install(ctx)
		es.CreateStream(ctx, "foo-stream")
		es.CreateStream(ctx, "bar-stream")

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo1"}, map[string]interface{}{}, time.Now()),
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo2"}, map[string]interface{}{}, time.Now()).WithVersion(2),
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo3"}, map[string]interface{}{}, time.Now()).WithVersion(3),
		})

		es.AppendTo(ctx, "bar-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(uuid.NewV4(), BarEvent{"Bar"}, map[string]interface{}{}, time.Now()),
		})

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo4"}, map[string]interface{}{}, time.Now()).WithVersion(4),
		})

		query := eventstore.NewQuery(es)
		query.
			Init(func() interface{} {
				return []string{}
			}).
			FromAll().
			When(map[string]eventstore.EventHandler{
				"FooEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					return append(state.([]string), event.Payload().(FooEvent).Foo), nil
				},
				"BarEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					return append(state.([]string), event.Payload().(BarEvent).Bar), nil
				},
			}).
			Run(ctx)

		state := query.State().([]string)

		if len(state) != 5 {
			t.Error("Query should return a list of all Event Payloads")
		}

		if state[0] != "Foo1" || state[1] != "Foo2" || state[2] != "Foo3" || state[3] != "Bar" || state[4] != "Foo4" {
			t.Error("Query should values in historical order")
		}
	})

	t.Run("Query with bool MetaMatcher", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		es := eventstore.NewEventStore(memory.NewPersistenceStrategy())
		es.Install(ctx)
		es.CreateStream(ctx, "foo-stream")

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo1"}, nil, time.Now()).WithAddedMetadata("first", true),
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo2"}, nil, time.Now()).WithVersion(2),
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo3"}, nil, time.Now()).WithVersion(3),
		})

		query := eventstore.NewQuery(es)
		query.
			Init(func() interface{} {
				return []string{}
			}).
			FromStream("foo-stream", []eventstore.MetadataMatch{
				{Field: "first", FieldType: eventstore.MetadataField, Operation: eventstore.EqualsOperator, Value: true},
			}).
			When(map[string]eventstore.EventHandler{
				"FooEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					return append(state.([]string), event.Payload().(FooEvent).Foo), nil
				},
			}).
			Run(ctx)

		state := query.State().([]string)

		if len(state) != 1 {
			t.Error("Query should return first event with required metadata")
		}

		if state[0] != "Foo1" {
			t.Error("Query should fetch requested event")
		}
	})

	t.Run("Query all Events from multiple streams with one handler until it stops", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		es := eventstore.NewEventStore(memory.NewPersistenceStrategy())
		es.Install(ctx)
		es.CreateStream(ctx, "foo-stream")
		es.CreateStream(ctx, "bar-stream")

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo1"}, map[string]interface{}{}, time.Now()),
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo2"}, map[string]interface{}{}, time.Now()).WithVersion(2),
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo3"}, map[string]interface{}{}, time.Now()).WithVersion(3),
		})

		es.AppendTo(ctx, "bar-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(uuid.NewV4(), BarEvent{"Bar"}, map[string]interface{}{}, time.Now()),
		})

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo4"}, map[string]interface{}{}, time.Now()).WithVersion(4),
		})

		query := eventstore.NewQuery(es)
		query.
			Init(func() interface{} {
				return []string{}
			}).
			FromStreams(eventstore.StreamProjection{StreamName: "foo-stream"}, eventstore.StreamProjection{StreamName: "bar-stream"}).
			WhenAny(func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
				switch e := event.Payload().(type) {
				case FooEvent:
					state = append(state.([]string), e.Foo)
				case BarEvent:
					query.Stop()
				}

				return state, nil
			}).
			Run(ctx)

		state := query.State().([]string)

		if len(state) != 3 {
			t.Error("Query should return a list of all Events before stop")
		}

		if state[0] != "Foo1" || state[1] != "Foo2" || state[2] != "Foo3" {
			t.Error("Query should values in historical order")
		}
	})

	t.Run("State is empty after reset", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		es := eventstore.NewEventStore(memory.NewPersistenceStrategy())
		es.Install(ctx)
		es.CreateStream(ctx, "foo-stream")

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo1"}, map[string]interface{}{}, time.Now()),
		})

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo2"}, map[string]interface{}{}, time.Now()).WithVersion(2),
		})

		query := eventstore.NewQuery(es)
		query.
			Init(func() interface{} {
				return []string{}
			}).
			FromStreams(eventstore.StreamProjection{StreamName: "foo-stream"}, eventstore.StreamProjection{StreamName: "bar-stream"}).
			WhenAny(func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
				switch e := event.Payload().(type) {
				case FooEvent:
					state = append(state.([]string), e.Foo)
				}

				return state, nil
			}).
			Run(ctx)

		err := query.Reset()
		if err != nil {
			t.Fatal(err)
		}

		state := query.State().([]string)

		if len(state) != 0 {
			t.Error("Projector should return an empty list after reset")
		}
	})

	t.Run("Process second Run", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		es := eventstore.NewEventStore(memory.NewPersistenceStrategy())
		es.Install(ctx)
		es.CreateStream(ctx, "foo-stream")
		es.CreateStream(ctx, "bar-stream")

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo1"}, map[string]interface{}{}, time.Now()),
		})

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo2"}, map[string]interface{}{}, time.Now()).WithVersion(2),
		})

		query := eventstore.NewQuery(es)
		query.
			Init(func() interface{} {
				return []string{}
			}).
			FromStreams(
				eventstore.StreamProjection{StreamName: "foo-stream"},
				eventstore.StreamProjection{StreamName: "bar-stream"},
			).
			When(map[string]eventstore.EventHandler{
				"FooEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					return append(state.([]string), event.Payload().(FooEvent).Foo), nil
				},
				"BarEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					return append(state.([]string), event.Payload().(BarEvent).Bar), nil
				},
			}).
			Run(ctx)

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo3"}, map[string]interface{}{}, time.Now()).WithVersion(3),
		})
		es.AppendTo(ctx, "bar-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, BarEvent{"Bar1"}, map[string]interface{}{}, time.Now()).WithVersion(1),
		})
		es.AppendTo(ctx, "bar-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, BarEvent{"Bar2"}, map[string]interface{}{}, time.Now()).WithVersion(2),
		})
		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo4"}, map[string]interface{}{}, time.Now()).WithVersion(4),
		})

		err := query.Run(ctx)
		if err != nil {
			t.Fatal(err)
		}

		state := query.State().([]string)

		if len(state) != 6 {
			t.Error("Projector should return complete list after second run")
		}
	})

	t.Run("Query return error returned by handler", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		es := eventstore.NewEventStore(memory.NewPersistenceStrategy())
		es.Install(ctx)
		es.CreateStream(ctx, "foo-stream")

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo1"}, map[string]interface{}{}, time.Now()),
		})

		query := eventstore.NewQuery(es)
		err := query.
			Init(func() interface{} {
				return []string{}
			}).
			FromStreams(
				eventstore.StreamProjection{StreamName: "foo-stream"},
				eventstore.StreamProjection{StreamName: "bar-stream"},
			).
			WhenAny(func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
				return state, errors.New("handler error")
			}).
			Run(ctx)

		if err == nil {
			t.Fatal("Should return handler error")
		}

		if err.Error() != "handler error" {
			t.Error("unexpected error")
		}
	})
}

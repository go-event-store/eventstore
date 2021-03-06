package eventstore_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	eventstore "github.com/go-event-store/eventstore"
	"github.com/go-event-store/eventstore/memory"
	uuid "github.com/satori/go.uuid"
)

func Test_Projector(t *testing.T) {
	type FooEvent struct {
		Foo string
	}

	type BarEvent struct {
		Bar string
	}

	beforeEach := func(ctx context.Context, aggregateID uuid.UUID) (eventstore.Projector, *eventstore.EventStore, eventstore.ProjectionManager) {
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

		pm := memory.NewProjectionManager()

		projector := eventstore.NewProjector("test", es, pm)
		projector.
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

		return projector, es, pm
	}

	t.Run("Project all Events", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		projector, _, pm := beforeEach(ctx, aggregateID)

		projector.
			FromStream("foo-stream", nil).
			Run(ctx, false)

		_, result, err := pm.LoadProjection(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}

		state := result.([]string)

		if len(state) != 5 {
			t.Fatal("Projection should return a list of all Event Payloads")
		}

		if state[0] != "Foo1" || state[1] != "Foo2" || state[2] != "Foo3" || state[3] != "Bar" || state[4] != "Foo4" {
			t.Error("Projection should return in historical order")
		}
	})

	t.Run("Project Events by Aggregate", func(t *testing.T) {
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

		projector, _, _ := beforeEach(ctx, aggregateID)
		projector.
			FromStream("foo-stream", matcher).
			Run(ctx, false)

		state := projector.State().([]string)

		if len(state) != 4 {
			t.Error("Projector should return a list of all Event of the given AggregateID")
		}

		if state[0] != "Foo1" || state[1] != "Foo2" || state[2] != "Foo3" || state[3] != "Foo4" {
			t.Error("Projector should values in historical order")
		}
	})

	t.Run("Project Events by Type and Version", func(t *testing.T) {
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

		projector, _, _ := beforeEach(ctx, aggregateID)
		projector.
			FromStream("foo-stream", matcher).
			Run(ctx, false)

		state := projector.State().([]string)

		if len(state) != 2 {
			t.Error("Projector should return events of type FooEvent and a Version greather than two")
		}

		if state[0] != "Foo3" || state[1] != "Foo4" {
			t.Error("Projector should values in historical order")
		}
	})

	t.Run("Reset Projector", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		projector, _, _ := beforeEach(ctx, aggregateID)

		projector.
			FromStream("foo-stream", nil).
			Run(ctx, false)

		projector.Reset(ctx)

		state := projector.State().([]string)

		if len(state) != 0 {
			t.Error("State should be resetted")
		}
	})

	t.Run("Project all Events from all streams", func(t *testing.T) {
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

		projector := eventstore.NewProjector("test", es, memory.NewProjectionManager())
		projector.
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
			Run(ctx, false)

		state := projector.State().([]string)

		if len(state) != 5 {
			t.Error("Projector should return a list of all Event Payloads")
		}

		if state[0] != "Foo1" || state[1] != "Foo2" || state[2] != "Foo3" || state[3] != "Bar" || state[4] != "Foo4" {
			t.Error("Projector should values in historical order")
		}
	})

	t.Run("Projector all Events from multiple streams with one handler until it stops", func(t *testing.T) {
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

		projector := eventstore.NewProjector("test", es, memory.NewProjectionManager())
		projector.
			Init(func() interface{} {
				return []string{}
			}).
			FromStreams(eventstore.StreamProjection{StreamName: "foo-stream"}, eventstore.StreamProjection{StreamName: "bar-stream"}).
			WhenAny(func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
				switch e := event.Payload().(type) {
				case FooEvent:
					state = append(state.([]string), e.Foo)
				case BarEvent:
					projector.Stop(ctx)
				}

				return state, nil
			}).
			Run(ctx, false)

		state := projector.State().([]string)

		if len(state) != 3 {
			t.Error("Projector should return a list of all Events before stop")
		}

		if state[0] != "Foo1" || state[1] != "Foo2" || state[2] != "Foo3" {
			t.Error("Projector should values in historical order")
		}
	})

	t.Run("Project all Events async", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		projector, _, _ := beforeEach(ctx, aggregateID)

		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {
			projector.
				FromStream("foo-stream", nil).
				Run(ctx, true)

			state := projector.State().([]string)

			if len(state) != 5 {
				t.Error("Projection should return a list of all Event Payloads")
			}

			if state[0] != "Foo1" || state[1] != "Foo2" || state[2] != "Foo3" || state[3] != "Bar" || state[4] != "Foo4" {
				t.Error("Projection should return in historical order")
			}

			wg.Done()
		}()

		time.Sleep(100 * time.Microsecond)

		projector.Stop(ctx)

		wg.Wait()
	})

	t.Run("State is empty after reset", func(t *testing.T) {
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

		projector := eventstore.NewProjector("test", es, memory.NewProjectionManager())
		projector.
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
			Run(ctx, false)

		err := projector.Reset(ctx)
		if err != nil {
			t.Fatal(err)
		}

		state := projector.State().([]string)

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

		projector := eventstore.NewProjector("test", es, memory.NewProjectionManager())
		projector.
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
			Run(ctx, false)

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

		err := projector.Run(ctx, false)
		if err != nil {
			t.Fatal(err)
		}

		state := projector.State().([]string)

		if len(state) != 6 {
			t.Error("Projector should return complete list after second run")
		}
	})

	t.Run("Process second Run with new Projector", func(t *testing.T) {
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

		pm := memory.NewProjectionManager()

		projector := eventstore.NewProjector("test", es, pm)
		projector.
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
			Run(ctx, false)

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

		projector2 := eventstore.NewProjector("test", es, pm)
		err := projector2.
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
			Run(ctx, false)

		if err != nil {
			t.Fatal(err)
		}

		state := projector2.State().([]string)

		if len(state) != 6 {
			t.Error("Projector should return complete list after second run")
		}
	})

	t.Run("Projector return error returned by handler", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		es := eventstore.NewEventStore(memory.NewPersistenceStrategy())
		es.Install(ctx)
		es.CreateStream(ctx, "foo-stream")

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo1"}, map[string]interface{}{}, time.Now()),
		})

		projector := eventstore.NewProjector("test", es, memory.NewProjectionManager())
		err := projector.
			Init(func() interface{} {
				return []string{}
			}).
			FromStreams(eventstore.StreamProjection{StreamName: "foo-stream"}, eventstore.StreamProjection{StreamName: "bar-stream"}).
			WhenAny(func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
				return state, errors.New("handler error")
			}).
			Run(ctx, false)

		if err == nil {
			t.Fatal("Should return handler error")
		}

		if err.Error() != "handler error" {
			t.Error("unexpected error")
		}
	})
}

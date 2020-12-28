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

type ReadModel struct {
	state map[string]string
}

func (r *ReadModel) Init(ctx context.Context) error {
	r.state = make(map[string]string)

	return nil
}

func (r *ReadModel) IsInitialized(ctx context.Context) (bool, error) {
	return r.state != nil, nil
}

func (r *ReadModel) Reset(ctx context.Context) error {
	r.state = make(map[string]string)

	return nil
}

func (r *ReadModel) Delete(ctx context.Context) error {
	r.state = make(map[string]string)

	return nil
}

func (r *ReadModel) Stack(method string, args ...map[string]interface{}) {
	r.state[args[0]["id"].(string)] = args[0]["name"].(string)
}

func (r *ReadModel) Persist(ctx context.Context) error {
	return nil
}

func Test_ReadModelProjector(t *testing.T) {
	type FooEvent struct {
		Foo string
	}

	type BarEvent struct {
		Bar string
	}

	beforeEach := func(ctx context.Context, aggregateID uuid.UUID) (eventstore.ReadModelProjector, *eventstore.EventStore, eventstore.ProjectionManager, *ReadModel) {
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
		rm := &ReadModel{}

		projector := eventstore.NewReadModelProjector("test", rm, es, pm)
		projector.
			Init(func() interface{} {
				return []string{}
			}).
			When(map[string]eventstore.EventHandler{
				"FooEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					projector.ReadModel.Stack("add", map[string]interface{}{
						"id":   event.AggregateID().String(),
						"name": event.Payload().(FooEvent).Foo,
					})

					return append(state.([]string), event.Payload().(FooEvent).Foo), nil
				},
				"BarEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					projector.ReadModel.Stack("add", map[string]interface{}{
						"id":   event.AggregateID().String(),
						"name": event.Payload().(BarEvent).Bar,
					})
					return append(state.([]string), event.Payload().(BarEvent).Bar), nil
				},
			})

		return projector, es, pm, rm
	}

	t.Run("Project all Events", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		projector, _, pm, rm := beforeEach(ctx, aggregateID)

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

		model := rm.state
		if len(model) != 2 {
			t.Fatal("ReadModel should return a list of all Aggregates")
		}

		aggregate := rm.state[aggregateID.String()]
		if aggregate != "Foo4" {
			t.Fatal("Unexpected aggregate name")
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

		projector, _, _, rm := beforeEach(ctx, aggregateID)
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

		model := rm.state
		if len(model) != 1 {
			t.Fatal("ReadModel should return a single filtered aggregate")
		}

		aggregate := rm.state[aggregateID.String()]
		if aggregate != "Foo4" {
			t.Fatal("Unexpected aggregate name")
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
				Operation: eventstore.LowerThanEuqalsOperator,
				Value:     2,
			},
		}

		projector, _, _, rm := beforeEach(ctx, aggregateID)
		projector.
			FromStream("foo-stream", matcher).
			Run(ctx, false)

		state := projector.State().([]string)

		if len(state) != 2 {
			t.Error("Projector should return events of type FooEvent and a Version greather than two")
		}

		if state[0] != "Foo1" || state[1] != "Foo2" {
			t.Error("Projector should values in historical order")
		}

		aggregate := rm.state[aggregateID.String()]
		if aggregate != "Foo2" {
			t.Fatal("Unexpected aggregate name")
		}
	})

	t.Run("Reset Projector", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		projector, _, _, rm := beforeEach(ctx, aggregateID)
		projector.
			FromStream("foo-stream", nil).
			Run(ctx, false)

		projector.Reset(ctx)

		state := projector.State().([]string)

		if len(state) != 0 {
			t.Error("State should be resetted")
		}

		model := rm.state
		if len(model) != 0 {
			t.Fatal("ReadModel should be resettet")
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

		rm := &ReadModel{}
		projector := eventstore.NewReadModelProjector("test", rm, es, memory.NewProjectionManager())
		projector.
			Init(func() interface{} {
				return []string{}
			}).
			FromAll().
			When(map[string]eventstore.EventHandler{
				"FooEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					projector.ReadModel.Stack("add", map[string]interface{}{
						"id":   event.AggregateID().String(),
						"name": event.Payload().(FooEvent).Foo,
					})

					return append(state.([]string), event.Payload().(FooEvent).Foo), nil
				},
				"BarEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					projector.ReadModel.Stack("add", map[string]interface{}{
						"id":   event.AggregateID().String(),
						"name": event.Payload().(BarEvent).Bar,
					})

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

		model := rm.state
		if len(model) != 2 {
			t.Fatal("ReadModel should return a list of all Aggregates")
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

		rm := &ReadModel{}
		projector := eventstore.NewReadModelProjector("test", rm, es, memory.NewProjectionManager())
		projector.
			Init(func() interface{} {
				return []string{}
			}).
			FromStreams(eventstore.StreamProjection{StreamName: "foo-stream"}, eventstore.StreamProjection{StreamName: "bar-stream"}).
			WhenAny(func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
				switch e := event.Payload().(type) {
				case FooEvent:
					projector.ReadModel.Stack("add", map[string]interface{}{
						"id":   event.AggregateID().String(),
						"name": e.Foo,
					})

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

		aggregate := rm.state[aggregateID.String()]
		if aggregate != "Foo3" {
			t.Fatal("Unexpected aggregate name")
		}
	})

	t.Run("Project all Events async", func(t *testing.T) {
		ctx := context.Background()
		aggregateID := uuid.NewV4()

		projector, _, _, rm := beforeEach(ctx, aggregateID)

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

			model := rm.state
			if len(model) != 2 {
				t.Error("ReadModel should return a list of all Aggregates")
			}

			aggregate := rm.state[aggregateID.String()]
			if aggregate != "Foo4" {
				t.Error("Unexpected aggregate name")
			}

			wg.Done()
		}()

		time.Sleep(100 * time.Microsecond)

		projector.Stop(ctx)

		wg.Wait()
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

		rm := &ReadModel{}
		projector := eventstore.NewReadModelProjector("test", rm, es, memory.NewProjectionManager())
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
					projector.ReadModel.Stack("add", map[string]interface{}{
						"id":   event.AggregateID().String(),
						"name": event.Payload().(FooEvent).Foo,
					})

					return append(state.([]string), event.Payload().(FooEvent).Foo), nil
				},
				"BarEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					projector.ReadModel.Stack("add", map[string]interface{}{
						"id":   event.AggregateID().String(),
						"name": event.Payload().(BarEvent).Bar,
					})

					return append(state.([]string), event.Payload().(BarEvent).Bar), nil
				},
			}).
			Run(ctx, false)

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo3"}, map[string]interface{}{}, time.Now()).WithVersion(3),
		})
		es.AppendTo(ctx, "bar-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(uuid.NewV4(), BarEvent{"Bar1"}, map[string]interface{}{}, time.Now()).WithVersion(1),
		})
		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo4"}, map[string]interface{}{}, time.Now()).WithVersion(4),
		})

		err := projector.Run(ctx, false)
		if err != nil {
			t.Fatal(err)
		}

		state := projector.State().([]string)
		if len(state) != 5 {
			t.Error("Projector should return complete list after second run")
		}

		model := rm.state
		if len(model) != 2 {
			t.Error("ReadModel should return a list of all Aggregates")
		}

		aggregate := rm.state[aggregateID.String()]
		if aggregate != "Foo4" {
			t.Error("Unexpected aggregate name")
		}
	})

	t.Run("Process second Run with new Projector", func(t *testing.T) {
		ctx := context.Background()
		fooID := uuid.NewV4()
		barID := uuid.NewV4()

		es := eventstore.NewEventStore(memory.NewPersistenceStrategy())
		es.Install(ctx)
		es.CreateStream(ctx, "foo-stream")
		es.CreateStream(ctx, "bar-stream")

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(fooID, FooEvent{"Foo1"}, map[string]interface{}{}, time.Now()),
		})

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(fooID, FooEvent{"Foo2"}, map[string]interface{}{}, time.Now()).WithVersion(2),
		})

		rm := &ReadModel{}
		pm := memory.NewProjectionManager()
		projector := eventstore.NewReadModelProjector("test", rm, es, pm)
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
					projector.ReadModel.Stack("add", map[string]interface{}{
						"id":   event.AggregateID().String(),
						"name": event.Payload().(FooEvent).Foo,
					})

					return append(state.([]string), event.Payload().(FooEvent).Foo), nil
				},
				"BarEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					projector.ReadModel.Stack("add", map[string]interface{}{
						"id":   event.AggregateID().String(),
						"name": event.Payload().(BarEvent).Bar,
					})

					return append(state.([]string), event.Payload().(BarEvent).Bar), nil
				},
			}).
			Run(ctx, false)

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(fooID, FooEvent{"Foo3"}, map[string]interface{}{}, time.Now()).WithVersion(3),
		})
		es.AppendTo(ctx, "bar-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(barID, BarEvent{"Bar1"}, map[string]interface{}{}, time.Now()).WithVersion(1),
		})
		es.AppendTo(ctx, "bar-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(barID, BarEvent{"Bar2"}, map[string]interface{}{}, time.Now()).WithVersion(2),
		})
		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(fooID, FooEvent{"Foo4"}, map[string]interface{}{}, time.Now()).WithVersion(4),
		})

		projector2 := eventstore.NewReadModelProjector("test", rm, es, pm)
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
					projector.ReadModel.Stack("add", map[string]interface{}{
						"id":   event.AggregateID().String(),
						"name": event.Payload().(FooEvent).Foo,
					})

					return append(state.([]string), event.Payload().(FooEvent).Foo), nil
				},
				"BarEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					projector.ReadModel.Stack("add", map[string]interface{}{
						"id":   event.AggregateID().String(),
						"name": event.Payload().(BarEvent).Bar,
					})

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

		model := rm.state
		if len(model) != 2 {
			t.Error("ReadModel should return a list of all Aggregates")
		}

		foo := rm.state[fooID.String()]
		if foo != "Foo4" {
			t.Error("Unexpected aggregate name")
		}

		bar := rm.state[barID.String()]
		if bar != "Bar2" {
			t.Error("Unexpected aggregate name")
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

		rm := &ReadModel{}
		projector := eventstore.NewReadModelProjector("test", rm, es, memory.NewProjectionManager())
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

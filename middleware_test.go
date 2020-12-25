package eventstore_test

import (
	"context"
	"testing"
	"time"

	eventstore "github.com/fjogeleit/go-event-store"
	"github.com/fjogeleit/go-event-store/memory"
	uuid "github.com/satori/go.uuid"
)

func setupMiddleware(ctx context.Context) *eventstore.EventStore {
	es := eventstore.NewEventStore(memory.NewPersistenceStrategy())
	es.Install(ctx)

	es.AppendMiddleware(eventstore.Loaded, func(ctx context.Context, event eventstore.DomainEvent) (eventstore.DomainEvent, error) {
		return event.WithAddedMetadata("version", event.Version()), nil
	})

	return es
}

func Test_EventStoreMiddleware(t *testing.T) {
	ctx := context.Background()

	t.Run("Loaded Events processed by Middleware", func(t *testing.T) {
		es := setupMiddleware(ctx)

		es.CreateStream(ctx, "foo-stream")

		type DomainEvent struct{}

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{eventstore.NewDomainEvent(uuid.NewV4(), DomainEvent{}, map[string]interface{}{}, time.Now())})

		iterator, err := es.Load(ctx, "foo-stream", 1, 0, nil)
		if err != nil {
			t.Error("Load should not fail")
		}

		events, _ := iterator.ToList()

		if len(events) != 1 {
			t.Fatal("Should load persisted events")
		}

		if _, ok := events[0].Metadata()["version"]; ok == false {
			t.Fatal("Should have a version Metadata from LoadedMiddleware")
		}

		if events[0].Metadata()["version"] != 1 {
			t.Fatal("Should have the Event version as value")
		}
	})

	t.Run("Load Events from multiple EventStreams", func(t *testing.T) {
		es := setupMiddleware(ctx)

		es.CreateStream(ctx, "foo-stream")
		es.CreateStream(ctx, "bar-stream")

		type FooEvent struct{}
		type BarEvent struct{}

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{eventstore.NewDomainEvent(uuid.NewV4(), FooEvent{}, map[string]interface{}{}, time.Now())})
		es.AppendTo(ctx, "bar-stream", []eventstore.DomainEvent{eventstore.NewDomainEvent(uuid.NewV4(), BarEvent{}, map[string]interface{}{}, time.Now())})

		iterator, err := es.MergeAndLoad(ctx, 0, eventstore.LoadStreamParameter{"foo-stream", 1, nil}, eventstore.LoadStreamParameter{"bar-stream", 1, nil})
		if err != nil {
			t.Error("MergeAndLoad should not fail")
		}

		events, _ := iterator.ToList()

		if len(events) != 2 {
			t.Error("Should load all persisted events from all streams")
		}

		if _, ok := events[0].Metadata()["version"]; ok == false {
			t.Fatal("Should have a version Metadata from LoadedMiddleware")
		}

		if events[0].Metadata()["version"] != 1 {
			t.Fatal("Should have the Event version as value")
		}

		if _, ok := events[1].Metadata()["version"]; ok == false {
			t.Fatal("Should have a version Metadata from LoadedMiddleware")
		}

		if events[1].Metadata()["version"] != 1 {
			t.Fatal("Should have the Event version as value")
		}
	})
}

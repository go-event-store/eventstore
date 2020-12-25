package eventstore_test

import (
	"context"
	"testing"
	"time"

	eventstore "github.com/fjogeleit/go-event-store"
	"github.com/fjogeleit/go-event-store/memory"
	uuid "github.com/satori/go.uuid"
)

func setup(ctx context.Context) *eventstore.EventStore {
	es := eventstore.NewEventStore(memory.NewPersistenceStrategy())
	es.Install(ctx)

	return es
}

func Test_InMemoryEventStore(t *testing.T) {
	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		setup(ctx)
	})

	t.Run("Create New Stream", func(t *testing.T) {
		es := setup(ctx)

		es.CreateStream(ctx, "foo-stream")

		if ok, _ := es.HasStream(ctx, "foo-stream"); ok == false {
			t.Error("Failed to create a new stream")
		}
	})

	t.Run("Create Stream Twice will fail", func(t *testing.T) {
		es := setup(ctx)

		es.CreateStream(ctx, "foo-stream")
		err := es.CreateStream(ctx, "foo-stream")

		if err == nil {
			t.Error("Create a EventStream twice should fail")
		}
	})

	t.Run("AppendTo EvenStream", func(t *testing.T) {
		es := setup(ctx)

		es.CreateStream(ctx, "foo-stream")

		err := es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{eventstore.NewDomainEvent(uuid.NewV4(), struct{}{}, map[string]interface{}{}, time.Now())})
		if err != nil {
			t.Error(err.Error())
		}
	})

	t.Run("Load all Events from a EvenStream", func(t *testing.T) {
		es := setup(ctx)

		es.CreateStream(ctx, "foo-stream")

		type FirstEvent struct{}
		type SecondEvent struct{}

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{eventstore.NewDomainEvent(uuid.NewV4(), FirstEvent{}, map[string]interface{}{}, time.Now())})
		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{eventstore.NewDomainEvent(uuid.NewV4(), SecondEvent{}, map[string]interface{}{}, time.Now())})

		iterator, err := es.Load(ctx, "foo-stream", 1, 0, nil)
		if err != nil {
			t.Error("Load should not fail")
		}

		events, _ := iterator.ToList()
		if len(events) != 2 {
			t.Error("Should load all persisted events")
		}

		if events[0].Name() != "FirstEvent" {
			t.Error("Should load all events in historical order")
		}
	})

	t.Run("Load with count returns only a subset", func(t *testing.T) {
		es := setup(ctx)

		es.CreateStream(ctx, "foo-stream")

		type FirstEvent struct{}
		type SecondEvent struct{}

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{eventstore.NewDomainEvent(uuid.NewV4(), FirstEvent{}, map[string]interface{}{}, time.Now())})
		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{eventstore.NewDomainEvent(uuid.NewV4(), SecondEvent{}, map[string]interface{}{}, time.Now())})

		iterator, err := es.Load(ctx, "foo-stream", 1, 1, nil)
		if err != nil {
			t.Error("Load should not fail")
		}

		events, _ := iterator.ToList()

		if len(events) != 1 {
			t.Error("Should load only one persisted event")
		}

		if events[0].Name() != "FirstEvent" {
			t.Error("Should load the first event")
		}

		iterator, err = es.Load(ctx, "foo-stream", 2, 1, nil)
		if err != nil {
			t.Error("Load should not fail")
		}

		events, _ = iterator.ToList()

		if len(events) != 1 {
			t.Error("Should load only one persisted event")
		}

		if events[0].Name() != "SecondEvent" {
			t.Error("Should load the second event")
		}
	})

	t.Run("Delete an existing EventStream", func(t *testing.T) {
		es := setup(ctx)

		es.CreateStream(ctx, "foo-stream")
		err := es.DeleteStream(ctx, "foo-stream")
		if err != nil {
			t.Error("Delete should not fail")
		}

		if ok, _ := es.HasStream(ctx, "foo-stream"); ok == true {
			t.Error("Failed to delete stream")
		}
	})

	t.Run("Delete a none existing EventStream should fail", func(t *testing.T) {
		es := setup(ctx)

		err := es.DeleteStream(ctx, "foo-stream")
		if err == nil {
			t.Error("Delete should fail if the given stream does not exist")
		}
	})

	t.Run("Load Events from multiple EventStreams", func(t *testing.T) {
		es := setup(ctx)

		es.CreateStream(ctx, "foo-stream")
		es.CreateStream(ctx, "bar-stream")

		type FooEvent struct{}
		type BarEvent struct{}

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{eventstore.NewDomainEvent(uuid.NewV4(), FooEvent{}, map[string]interface{}{}, time.Now())})
		es.AppendTo(ctx, "bar-stream", []eventstore.DomainEvent{eventstore.NewDomainEvent(uuid.NewV4(), BarEvent{}, map[string]interface{}{}, time.Now())})
		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{eventstore.NewDomainEvent(uuid.NewV4(), FooEvent{}, map[string]interface{}{}, time.Now())})

		iterator, err := es.MergeAndLoad(ctx, 0, eventstore.LoadStreamParameter{"foo-stream", 1, nil}, eventstore.LoadStreamParameter{"bar-stream", 1, nil})
		if err != nil {
			t.Error("MergeAndLoad should not fail")
		}

		events, _ := iterator.ToList()

		if len(events) != 3 {
			t.Error("Should load all persisted events from all streams")
		}

		if events[0].Name() != "FooEvent" && events[1].Name() != "BarEvent" && events[2].Name() != "FooEvent" {
			t.Error("Should load all events in historical order")
		}
	})

	t.Run("Load a subset of Events from multiple EventStreams", func(t *testing.T) {
		es := setup(ctx)

		es.CreateStream(ctx, "foo-stream")
		es.CreateStream(ctx, "bar-stream")

		type FooEvent struct{}
		type BarEvent struct{}

		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{eventstore.NewDomainEvent(uuid.NewV4(), FooEvent{}, map[string]interface{}{}, time.Now())})
		es.AppendTo(ctx, "bar-stream", []eventstore.DomainEvent{eventstore.NewDomainEvent(uuid.NewV4(), BarEvent{}, map[string]interface{}{}, time.Now())})
		es.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{eventstore.NewDomainEvent(uuid.NewV4(), FooEvent{}, map[string]interface{}{}, time.Now())})

		iterator, err := es.MergeAndLoad(ctx, 2, eventstore.LoadStreamParameter{"foo-stream", 1, nil}, eventstore.LoadStreamParameter{"bar-stream", 1, nil})
		if err != nil {
			t.Error("MergeAndLoad should not fail")
		}

		events, _ := iterator.ToList()

		if len(events) != 2 {
			t.Error("Should load a subset of 2 persisted events from all streams")
		}

		if events[0].Name() != "FooEvent" && events[1].Name() != "BarEvent" {
			t.Error("Should load all events in historical order")
		}
	})

	t.Run("FetchAllStreamNames returns a list of all existing event streams", func(t *testing.T) {
		es := setup(ctx)

		es.CreateStream(ctx, "foo-stream")
		es.CreateStream(ctx, "bar-stream")
		es.CreateStream(ctx, "baz-stream")

		list, _ := es.FetchAllStreamNames(ctx)

		if len(list) != 3 {
			t.Error("Should return a list of all EventStreams")
		}
	})
}

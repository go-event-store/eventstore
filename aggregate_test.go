package eventstore_test

import (
	"context"
	"testing"

	eventstore "github.com/fjogeleit/go-event-store"
	"github.com/fjogeleit/go-event-store/memory"
)

type BazWasCreated struct {
	Baz string
}

type BazWasUpdated struct {
	Baz string
}

type BazAggregate struct {
	eventstore.BaseAggregate

	Baz string
}

func (b *BazAggregate) Create(baz string) {
	b.RecordThat(BazWasCreated{baz}, nil)
}

func (b *BazAggregate) Update(baz string) {
	b.RecordThat(BazWasUpdated{baz}, map[string]interface{}{
		"meta": 1,
	})
}

func (b *BazAggregate) WhenBazWasCreated(e BazWasCreated, metadata map[string]interface{}) {
	b.Baz = e.Baz
}

func (b *BazAggregate) WhenBazWasUpdated(e BazWasUpdated, metadata map[string]interface{}) {
	b.Baz = e.Baz
}

func NewBazAggregate() *BazAggregate {
	aggregate := new(BazAggregate)
	aggregate.BaseAggregate = eventstore.NewAggregate(aggregate)

	return aggregate
}

func Test_Aggregate(t *testing.T) {
	typeRegistry := eventstore.NewTypeRegistry()
	typeRegistry.RegisterAggregate(BazAggregate{})
	typeRegistry.RegisterEvents(BazWasCreated{}, BazWasUpdated{})

	t.Run("Create Aggregate", func(t *testing.T) {
		aggregate := NewBazAggregate()
		aggregate.Create("Baz")

		newEvents := aggregate.PopEvents()

		if aggregate.Baz != "Baz" {
			t.Error("Should set the baz to the givent event value")
		}

		if len(newEvents) != 1 {
			t.Error("Should create a new event to the stack")
		}

		event := newEvents[0]

		if event.AggregateType() != "BazAggregate" {
			t.Error("Should set the related AggregateType to the event metadata")
		}

		if event.Version() != 1 {
			t.Error("Should set the correct event version")
		}

		if event.AggregateID() != aggregate.AggregateID() {
			t.Error("Should set the correct AggregateID")
		}
	})

	t.Run("Create and Update Aggregate", func(t *testing.T) {
		aggregate := NewBazAggregate()
		aggregate.Create("Baz")
		aggregate.Update("Foo")

		newEvents := aggregate.PopEvents()

		if aggregate.Baz != "Foo" {
			t.Error("Should set the baz to the givent event value")
		}

		if len(newEvents) != 2 {
			t.Error("Should create a new event to the stack")
		}

		updateEvent := newEvents[1]

		if updateEvent.Name() != "BazWasUpdated" {
			t.Error("Last Event should also be the last list item")
		}

		if updateEvent.Version() != 2 {
			t.Error("Should set the correct aggregate version to the next event")
		}
	})

	t.Run("Recreate Aggregate FromHistory", func(t *testing.T) {
		aggregate := NewBazAggregate()
		aggregate.Create("Baz")
		aggregate.Update("Foo")

		events := aggregate.PopEvents()

		aggregateCopy := NewBazAggregate()
		aggregateCopy.FromHistory(memory.NewDomainEventIterator(context.Background(), events))

		if aggregateCopy.AggregateID() != aggregate.AggregateID() {
			t.Error("Copy should have the same AggregateID")
		}

		if aggregateCopy.Baz != aggregate.Baz {
			t.Error("Copy should have the same state")
		}
	})
}

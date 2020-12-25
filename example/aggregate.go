package example

import eventstore "github.com/go-event-store/eventstore"

type FooEvent struct {
	Foo string
}

type BarEvent struct {
	Bar string
}

type FooAggregate struct {
	eventstore.BaseAggregate

	Foo string
}

func (f *FooAggregate) WhenFooEvent(e FooEvent) {
	f.Foo = e.Foo
}

func NewFooAggregate() *FooAggregate {
	fooAggregate := new(FooAggregate)
	fooAggregate.BaseAggregate = eventstore.NewAggregate(fooAggregate)

	return fooAggregate
}

func NewFooAggregateFromHistory(events eventstore.DomainEventIterator) *FooAggregate {
	aggregate := new(FooAggregate)
	aggregate.BaseAggregate = eventstore.NewAggregate(aggregate)

	aggregate.FromHistory(events)

	return aggregate
}

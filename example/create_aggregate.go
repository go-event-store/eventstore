package example

import (
	"context"
	"fmt"

	eventstore "github.com/fjogeleit/go-event-store"
	"github.com/fjogeleit/go-event-store/memory"
)

func CreateAggregate(ctx context.Context) {
	ps := memory.NewPersistenceStrategy()
	es := eventstore.NewEventStore(ps)

	err := es.Install(ctx)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	err = es.CreateStream(ctx, FooStream)
	if err != nil {
		fmt.Println(err.Error())
	}

	//es.AppendMiddleware(eventstore.PreAppend, Transform)
	//es.AppendMiddleware(eventstore.Loaded, Logger)

	typeRegistry := eventstore.NewTypeRegistry()
	typeRegistry.RegisterAggregate(&FooAggregate{}, FooEvent{})

	fooAggregate := NewFooAggregate()
	fooAggregate.RecordThat(FooEvent{Foo: "Bar"}, make(map[string]interface{}, 0))
	fooAggregate.RecordThat(FooEvent{Foo: "Baz"}, make(map[string]interface{}, 0))
	fooAggregate.RecordThat(FooEvent{Foo: "Foo"}, make(map[string]interface{}, 0))
	fooAggregate.RecordThat(FooEvent{Foo: "Fat"}, make(map[string]interface{}, 0))
	fooAggregate.RecordThat(FooEvent{Foo: "Fou"}, make(map[string]interface{}, 0))

	fooAggregate2 := NewFooAggregate()
	fooAggregate2.RecordThat(FooEvent{Foo: "Bit"}, make(map[string]interface{}, 0))
	fooAggregate2.RecordThat(FooEvent{Foo: "Bat"}, make(map[string]interface{}, 0))

	repo := NewFooRepository(FooStream, es)

	err = repo.Save(ctx, fooAggregate)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	err = repo.Save(ctx, fooAggregate2)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	result, err := repo.Get(ctx, fooAggregate.AggregateID())
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Printf("Aggregate %T {Foo: %s}\n", result, result.Foo)
}

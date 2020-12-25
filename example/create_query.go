package example

import (
	"context"
	"fmt"

	eventstore "github.com/fjogeleit/go-event-store"
	"github.com/fjogeleit/go-event-store/memory"
)

func CreateQuery(ctx context.Context) {
	typeRegistry := eventstore.NewTypeRegistry()
	typeRegistry.RegisterAggregate(FooAggregate{})
	typeRegistry.RegisterEvents(FooEvent{}, BarEvent{})

	ps := memory.NewPersistenceStrategy()
	es := eventstore.NewEventStore(ps)

	query := eventstore.NewQuery(es)
	err := query.
		FromStream(FooStream, []eventstore.MetadataMatch{}).
		Init(func() interface{} {
			return []string{}
		}).
		When(map[string]func(state interface{}, event eventstore.DomainEvent) interface{}{
			FooEventName: func(state interface{}, event eventstore.DomainEvent) interface{} {
				return append(state.([]string), event.Payload().(FooEvent).Foo)
			},
			BarEventName: func(state interface{}, event eventstore.DomainEvent) interface{} {
				return append(state.([]string), event.Payload().(BarEvent).Bar)
			},
		}).
		Run(ctx)

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(query.State())
}

package example

import (
	"context"
	"fmt"

	eventstore "github.com/go-event-store/eventstore"
	"github.com/go-event-store/eventstore/memory"
)

func CreateProjection(ctx context.Context) {
	typeRegistry := eventstore.NewTypeRegistry()
	typeRegistry.RegisterAggregate(FooAggregate{})
	typeRegistry.RegisterEvents(FooEvent{}, BarEvent{})

	ps := memory.NewPersistenceStrategy()
	es := eventstore.NewEventStore(ps)
	pm := memory.NewProjectionManager()

	projector := eventstore.NewProjector("foo_projection", es, pm)
	err := projector.
		FromStream(FooStream, []eventstore.MetadataMatch{}).
		Init(func() interface{} {
			return []string{}
		}).
		When(map[string]eventstore.EventHandler{
			FooEventName: func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
				foo := event.Payload().(FooEvent).Foo
				nextState := []string{}

				switch s := state.(type) {
				case []interface{}:
					for _, v := range s {
						nextState = append(nextState, fmt.Sprint(v))
					}
				case []string:
					nextState = s
				}

				return append(nextState, foo), nil
			},
			BarEventName: func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
				bar := event.Payload().(BarEvent).Bar
				nextState := []string{}

				switch s := state.(type) {
				case []interface{}:
					for _, v := range s {
						nextState = append(nextState, fmt.Sprint(v))
					}
				case []string:
					nextState = s
				}

				return append(nextState, bar), nil
			},
		}).
		Run(ctx, false)

	if err != nil {
		fmt.Println(err)
		return
	}
}

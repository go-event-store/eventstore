package example

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	eventstore "github.com/go-event-store/eventstore"
)

func Logger(_ context.Context, event eventstore.DomainEvent) (eventstore.DomainEvent, error) {
	payload, err := json.Marshal(event.Payload())
	if err != nil {
		return event, nil
	}

	fmt.Printf("[%s] %s: Payload: %s\n", event.CreatedAt().Format(time.RFC3339), event.Name(), payload)

	return event, nil
}

func Transform(_ context.Context, event eventstore.DomainEvent) (eventstore.DomainEvent, error) {
	return eventstore.NewDomainEvent(event.AggregateID(), FooEvent{"TRANSFORMED"}, event.Metadata(), event.CreatedAt()), nil
}

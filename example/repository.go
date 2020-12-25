package example

import (
	"context"
	"errors"

	eventstore "github.com/go-event-store/eventstore"
	uuid "github.com/satori/go.uuid"
)

var ErrFooNotFound = errors.New("Foo not found")

type FooRepository struct {
	rootRepo eventstore.Repository
}

func (r FooRepository) Get(ctx context.Context, fooID uuid.UUID) (*FooAggregate, error) {
	events, err := r.rootRepo.GetAggregate(ctx, fooID)
	if err != nil {
		return nil, err
	}

	empty, err := events.IsEmpty()
	if err != nil {
		return nil, err
	}
	if empty {
		return nil, ErrFooNotFound
	}

	return NewFooAggregateFromHistory(events), nil
}

func (r FooRepository) Save(ctx context.Context, foo *FooAggregate) error {
	return r.rootRepo.SaveAggregate(ctx, foo)
}

func NewFooRepository(streamName string, eventStore *eventstore.EventStore) *FooRepository {
	return &FooRepository{
		rootRepo: eventstore.NewRepository(streamName, eventStore),
	}
}

package memory

import (
	"context"

	eventstore "github.com/go-event-store/eventstore"
)

type DomainEventIterator struct {
	position int
	ctx      context.Context
	current  *eventstore.DomainEvent
	events   []eventstore.DomainEvent
	err      error
}

func (it *DomainEventIterator) Next() bool {
	it.position++

	if len(it.events) >= it.position+1 {
		it.current = &it.events[it.position]
		return true
	}

	it.Close()

	return false
}

func (it *DomainEventIterator) Current() (*eventstore.DomainEvent, error) {
	if it.position == -1 {
		it.Next()
	}

	return it.current, nil
}

func (it *DomainEventIterator) Rewind() {
	it.current = nil
	it.position = -1
}

func (it *DomainEventIterator) Error() error {
	return it.err
}

func (it *DomainEventIterator) Close() {
	it.current = nil
	it.events = make([]eventstore.DomainEvent, 0)
}

func (it *DomainEventIterator) IsEmpty() (bool, error) {
	it.Next()

	return len(it.events) == 0, nil
}

func (it *DomainEventIterator) ToList() ([]eventstore.DomainEvent, error) {
	list := []eventstore.DomainEvent{}

	for it.Next() {
		list = append(list, *it.current)
	}

	return list, nil
}

func NewDomainEventIterator(ctx context.Context, events []eventstore.DomainEvent) *DomainEventIterator {
	return &DomainEventIterator{
		ctx:      ctx,
		position: -1,
		events:   events,
	}
}

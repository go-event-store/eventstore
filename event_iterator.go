package eventstore

import (
	"context"
	"fmt"
)

// DomainEventIterator is a lazy loading Iterator
// It fetches DomainEvents in 1000er steps until all events are loaded
type DomainEventIterator interface {
	// Next turns the cursor to the next DomainEvent
	Next() bool
	// Current returns the current selected DomainEvent in the List or a related error
	Current() (*DomainEvent, error)
	// Error returns the latest error
	Error() error
	// Rewind the iterator cursor
	Rewind()
	// Close removes all fetched events and resets the cursor
	Close()
	// IsEmpty checks for any DomainEvent in the Iterator
	IsEmpty() (bool, error)
	// Converts the Iterator to an list of DomainEvents
	ToList() ([]DomainEvent, error)
}

type MiddlewareIterator struct {
	ctx        context.Context
	iterator   DomainEventIterator
	middleware []DomainEventMiddleware
}

func (m MiddlewareIterator) Next() bool {
	return m.iterator.Next()
}

func (m MiddlewareIterator) Current() (*DomainEvent, error) {
	if len(m.middleware) == 0 {
		return m.iterator.Current()
	}

	event, err := m.iterator.Current()
	if err != nil {
		return nil, err
	}

	modifedEvent, err := m.executeMiddleware(*event)

	return &modifedEvent, err
}

func (m MiddlewareIterator) Error() error {
	return m.iterator.Error()
}

func (m MiddlewareIterator) IsEmpty() (bool, error) {
	return m.iterator.IsEmpty()
}

func (m MiddlewareIterator) Rewind() {
	m.iterator.Rewind()
}

func (m MiddlewareIterator) Close() {
	m.iterator.Close()
}

func (m MiddlewareIterator) ToList() ([]DomainEvent, error) {
	list := []DomainEvent{}

	for m.Next() {
		event, err := m.Current()
		if err != nil {
			return list, err
		}

		list = append(list, *event)
	}

	return list, nil
}

func (m MiddlewareIterator) executeMiddleware(event DomainEvent) (DomainEvent, error) {
	var err error

	if len(m.middleware) > 0 {
		for _, middleware := range m.middleware {
			event, err = middleware(m.ctx, event)
			if err != nil {
				return event, fmt.Errorf("Failed to process Loaded Middleware: %s", err.Error())
			}
		}
	}

	return event, nil
}

func NewMiddlewareIterator(ctx context.Context, iterator DomainEventIterator, middleware []DomainEventMiddleware) MiddlewareIterator {
	return MiddlewareIterator{ctx, iterator, middleware}
}

package eventstore

import "context"

type DomainEventAction = string

const (
	PreAppend DomainEventAction = "PRE_APPEND"
	Appended  DomainEventAction = "APPENDED"
	Loaded    DomainEventAction = "LOADED"
)

type DomainEventMiddleware = func(ctx context.Context, event DomainEvent) (DomainEvent, error)

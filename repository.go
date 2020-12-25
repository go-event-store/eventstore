package eventstore

import (
	"context"

	uuid "github.com/satori/go.uuid"
)

// Repository for an AggregateType
type Repository struct {
	// Stream releated to your AggregateType
	Stream string
	// EventStore releated to your Stream
	es *EventStore
}

// GetAggregate returns a list of all persisted events of a single Aggregate, grouped by the AggregateID, in historical order
func (r Repository) GetAggregate(ctx context.Context, aggregateID uuid.UUID) (DomainEventIterator, error) {
	return r.es.Load(ctx, r.Stream, 1, 0, []MetadataMatch{{Field: "_aggregate_id", Value: aggregateID.String(), Operation: EqualsOperator, FieldType: MetadataField}})
}

// SaveAggregate persist all new Events to the EventStore
func (r Repository) SaveAggregate(ctx context.Context, aggregate Aggregate) error {
	return r.es.AppendTo(ctx, r.Stream, aggregate.PopEvents())
}

// NewRepository creates a Repository
func NewRepository(streamName string, eventStore *EventStore) Repository {
	return Repository{
		streamName,
		eventStore,
	}
}

package memory

import (
	"context"
	"regexp"
	"sort"
	"time"

	eventstore "github.com/go-event-store/eventstore"
)

type PersistenceStrategy struct {
	streams map[string][]eventstore.DomainEvent
}

func (ps *PersistenceStrategy) CreateEventStreamsTable(_ context.Context) error {
	ps.streams = map[string][]eventstore.DomainEvent{}

	return nil
}

func (ps *PersistenceStrategy) CreateProjectionsTable(_ context.Context) error {
	return nil
}

func (ps *PersistenceStrategy) AddStreamToStreamsTable(_ context.Context, streamName string) error {
	if _, ok := ps.streams[streamName]; ok == false {
		ps.streams[streamName] = []eventstore.DomainEvent{}

		return nil
	}

	return eventstore.StreamAlreadyExist{Stream: streamName}
}

func (ps *PersistenceStrategy) RemoveStreamFromStreamsTable(_ context.Context, streamName string) error {
	if _, ok := ps.streams[streamName]; ok == true {
		delete(ps.streams, streamName)

		return nil
	}

	return eventstore.StreamAlreadyExist{Stream: streamName}
}

func (ps *PersistenceStrategy) FetchAllStreamNames(_ context.Context) ([]string, error) {
	streams := []string{}

	for stream := range ps.streams {
		streams = append(streams, stream)
	}

	return streams, nil
}

func (ps *PersistenceStrategy) HasStream(_ context.Context, streamName string) (bool, error) {
	_, ok := ps.streams[streamName]

	return ok, nil
}

func (ps *PersistenceStrategy) DeleteStream(ctx context.Context, streamName string) error {
	return ps.RemoveStreamFromStreamsTable(ctx, streamName)
}

func (ps *PersistenceStrategy) CreateSchema(_ context.Context, _ string) error {
	return nil
}

func (ps *PersistenceStrategy) DropSchema(_ context.Context, _ string) error {
	return nil
}

func (ps *PersistenceStrategy) AppendTo(ctx context.Context, streamName string, events []eventstore.DomainEvent) error {
	if ok, _ := ps.HasStream(ctx, streamName); ok == false {
		return eventstore.StreamNotFound{Stream: streamName}
	}

	nextNumber := len(ps.streams[streamName])

	for _, event := range events {
		nextNumber++
		ps.streams[streamName] = append(ps.streams[streamName], event.WithNumber(nextNumber))
	}

	return nil
}

func (ps *PersistenceStrategy) Load(ctx context.Context, streamName string, fromNumber, count int, matcher eventstore.MetadataMatcher) (eventstore.DomainEventIterator, error) {
	if ok, _ := ps.HasStream(ctx, streamName); ok == false {
		return nil, eventstore.StreamNotFound{Stream: streamName}
	}

	from := fromNumber - 1
	to := from + count

	if count == 0 {
		return NewDomainEventIterator(ctx, ps.filter(ps.streams[streamName][from:], matcher)), nil
	}

	return NewDomainEventIterator(ctx, ps.filter(ps.streams[streamName][from:to], matcher)), nil
}

func (ps *PersistenceStrategy) MergeAndLoad(ctx context.Context, count int, streams ...eventstore.LoadStreamParameter) (eventstore.DomainEventIterator, error) {
	events := []eventstore.DomainEvent{}

	for _, stream := range streams {
		number := 0
		if stream.FromNumber > 0 {
			number = stream.FromNumber - 1
		}

		events = append(events, ps.filter(ps.streams[stream.StreamName][number:], stream.Matcher)...)
	}

	sort.SliceStable(events, func(i, j int) bool {
		return events[i].CreatedAt().Before(events[j].CreatedAt())
	})

	if count > 0 {
		return NewDomainEventIterator(ctx, events[:count]), nil
	}

	return NewDomainEventIterator(ctx, events), nil
}

func (ps PersistenceStrategy) filter(events []eventstore.DomainEvent, matcher eventstore.MetadataMatcher) []eventstore.DomainEvent {
	if len(matcher) == 0 {
		return events
	}

	base := events

	for _, match := range matcher {
		result := []eventstore.DomainEvent{}

		if match.FieldType == eventstore.MetadataField {
			for _, event := range base {
				if value, ok := event.Metadata()[match.Field]; ok {
					if ps.match(value, match.Value, match.Operation) {
						result = append(result, event)
					}
				} else {
					if ps.match(nil, match.Value, match.Operation) {
						result = append(result, event)
					}
				}
			}
		}

		if match.FieldType == eventstore.MessagePropertyField {
			for _, event := range base {
				var matched bool

				switch match.Field {
				case "createdAt":
					matched = ps.match(event.CreatedAt(), match.Value, match.Operation)
				case "created_at":
					matched = ps.match(event.CreatedAt(), match.Value, match.Operation)
				case "event_name":
					matched = ps.match(event.Name(), match.Value, match.Operation)
				case "name":
					matched = ps.match(event.Name(), match.Value, match.Operation)
				case "uuid":
					matched = ps.match(event.UUID().String(), match.Value, match.Operation)
				default:
					matched = false
				}

				if matched {
					result = append(result, event)
				}
			}
		}

		base = result
	}

	return base
}

func (ps PersistenceStrategy) match(value interface{}, expected interface{}, operation eventstore.MetadataOperator) bool {
	switch operation {
	case eventstore.InOperator:
		return findIn(expected.([]interface{}), value)
	case eventstore.NotInOperator:
		return findIn(expected.([]interface{}), value) == false
	case eventstore.RegexOperator:
		matched, err := regexp.MatchString(expected.(string), value.(string))
		if err != nil {
			return false
		}

		return matched
	case eventstore.EqualsOperator:
		return expected == value
	case eventstore.NotEqualsOperator:
		return expected != value
	case eventstore.LowerThanOperator:
		switch v := value.(type) {
		case int:
			return expected.(int) > v
		case float64:
			return expected.(float64) > v
		case time.Time:
			return expected.(time.Time).Before(v)
		default:
			return false
		}
	case eventstore.GreaterThanOperator:
		switch v := value.(type) {
		case int:
			return expected.(int) < v
		case float64:
			return expected.(float64) < v
		case time.Time:
			return expected.(time.Time).After(v)
		default:
			return false
		}
	case eventstore.LowerThanEuqalsOperator:
		switch v := value.(type) {
		case int:
			return expected.(int) >= v
		case float64:
			return expected.(float64) >= v
		case time.Time:
			return expected.(time.Time).After(v) || expected.(time.Time).Equal(v)
		default:
			return false
		}
	case eventstore.GreaterThanEqualsOperator:
		switch v := value.(type) {
		case int:
			return expected.(int) <= v
		case float64:
			return expected.(float64) <= v
		case time.Time:
			return expected.(time.Time).Before(v) || expected.(time.Time).Equal(v)
		default:
			return false
		}
	}

	return false
}

func NewPersistenceStrategy() *PersistenceStrategy {
	return &PersistenceStrategy{}
}

func findIn(slice []interface{}, val interface{}) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

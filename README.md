# GO Event Store

![Test Workflow](https://github.com/fjogeleit/go-event-store/workflows/Workflow/badge.svg)

This Library is an EventStore heavily inspired by the prooph/event-store v7.0.

## Provider

- Postgres
- MySQL
- InMemory

## Implemented:

- **SingleStream** Strategy: Create a Stream for each Aggregate
- Loading and saving Aggregates
- Persistent Projections
- ReadModel Projections
- Event Queries

### Projections / Queries

- You can query and process one or multiple Streams with the `FromStream`, `FromStreams`, `FromAll` API.
- Fetch all or a subset of Events with an optional `MetadataMatcher`
- Create persisted State with an `Projector` or temporary created State with a `Query`
- Fetching multiple streams creates a merged stream and run over the events in historical order

## Examples

See the [GO EventStore Example](https://github.com/fjogeleit/go-event-store-example) Repository for a basic integration in the Gin Framework

### Initialize the EventStore

```go
ctx := context.Background()

pool, err := pgxpool.Connect(ctx, DB_URL)
if err != nil {
    fmt.Println(err.Error())
    return
}

// choose your persistence strategy
ps := pg.NewPersistenceStrategy(pool)

// create the event store
es := eventstore.NewEventStore(ps)

// initialize the event store
// creates the event_streams table to persist all created eventstreams
// creates the projection table to store all created persisted projections
err = es.Install(ctx)
if err != nil {
    fmt.Println(err.Error())
    return
}

// add a new eventstream to the event_streams table
// creates a  new table _{sha1 of streamName} for events
err = es.CreateStream(ctx, "foo-stream")
if err != nil {
    fmt.Println(err.Error())
}
```

### Create an Aggregate with the related Events

```go
const (
	FooStream = "foo-stream"

	FooEventName = "FooEvent"
	BarEventName = "BarEvent"
)

// Each Event is an serializable struct and will be included as Payload in the PersistedEvent
type FooEvent struct {
	Foo string
}

type BarEvent struct {
	Bar string
}

// Create a new Aggregate with the Help of the BaseAggregate struct
type FooAggregate struct {
	eventstore.BaseAggregate

    // custom fields who represent the latest state
	Foo string
}

// An aggregate should have a EventHandler with the schema when${EventName}
// This handler will automaticlly called when an event is recorded
func (f *FooAggregate) WhenFooEvent(e FooEvent, metadata map[string]interface{}) {
	f.Foo = e.Foo
}

// Constructor Function which use the helper constructor function for the BaseAggregate
func NewFooAggregate() *FooAggregate {
	fooAggregate := new(FooAggregate)
	fooAggregate.BaseAggregate = eventstore.NewAggregate(fooAggregate)

	return fooAggregate
}

// Constructor Function which reconstructor an Aggregate with the event history
func NewFooAggregateFromHistory(events eventstore.DomainEventIterator) *FooAggregate {
	aggregate := new(FooAggregate)
	aggregate.BaseAggregate = eventstore.NewAggregate(aggregate)
	aggregate.FromHistory(events)

	return aggregate
}
```

### Load and save Aggregates with an Repositories

```go
type FooRepository struct {
	rootRepo eventstore.Repository
}

// use the history constructor to load an existing Aggregate
func (r FooRepository) Get(ctx context.Context, fooID uuid.UUID) (*FooAggregate, error) {
	events, err := r.rootRepo.GetAggregate(ctx, fooID)
	if err != nil {
		return nil, err
	}

	return NewFooAggregateFromHistory(events), nil
}

// persist all new recorded events
func (r FooRepository) Save(ctx context.Context, foo *FooAggregate) error {
	return r.rootRepo.SaveAggregate(ctx, foo)
}

func NewFooRepository(streamName string, eventStore eventstore.EventStore) FooRepository {
	return FooRepository{
		rootRepo: eventstore.NewRepository(streamName, eventStore),
	}
}
```

### Use it all together

```go
err = es.CreateStream(ctx, FooStream)
if err != nil {
    fmt.Println(err.Error())
    return
}

// register all existing aggregates and events
typeRegistry := eventstore.NewTypeRegistry()
typeRegistry.RegisterAggregate(FooAggregate{})
typeRegistry.RegisterEvents(FooEvent{})

// create a new instance of an aggregate
fooAggregate := NewFooAggregate()

// record new events with the RecordThat method, record additional metadata to your event
fooAggregate.RecordThat(FooEvent{Foo: "Bar"}, nil)
fooAggregate.RecordThat(FooEvent{Foo: "Baz"}, map[string]interface{}{"meta":"data"})

// create a repository and save the aggregate
repo := NewFooRepository(FooStream, es)
err = repo.Save(ctx, fooAggregate)
if err != nil {
    fmt.Println(err.Error())
    return
}

// reload it from the database
result, err := repo.Get(ctx, fooAggregate.AggregateID())
if err != nil {
    fmt.Println(err.Error())
    return
}
```

## Create and using Queries and persisted Projections

### Query from an EventStream

```go
// Register existing aggregates and events
typeRegistry := eventstore.NewTypeRegistry()
typeRegistry.RegisterAggregate(FooAggregate{})
typeRegistry.RegisterEvents(FooEvent{}, BarEvent{})

ps := pg.NewPersistenceStrategy(pool)
es := eventstore.NewEventStore(ps)

query := eventstore.NewQuery(es)
err = query.
    // read from a single stream
    FromStream(FooStream, []eventstore.MetadataMatch{}).
    // init your state
    Init(func() interface{} {
        return []string{}
    }).
    // define a callback for each possible event
    // events without a handler will be ignored
    // with WhenAny you can also define a single handler for all events
    // Key of the Map is the EventName without package
    // A handler receives the current state as first argument and the wrapped event as second argument
    // it returns the new state which will be the first argument of the next handler call
    // Access your EventStruct with event.Payload(), you can access additional information from the persistedEvent wrapper like
    //  CreatedAt
    //  AggregateID
    //  AggregateType
    //  Unique EventID (UUID)
    //  Custom Metadata
    When(map[string]func(state interface{}, event eventstore.DomainEvent) interface{} {
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

// Access the result of the Query
fmt.Println(query.State())
```

### ReadModels from an EventStream

### Define an ReadModel

You can use the helper Client to execute DB Operations

```go
type FooReadModel struct {
	client *pg.Client
	stack  []struct {
		method string
		args   []map[string]interface{}
	}
}

func (f *FooReadModel) Init(ctx context.Context) error {
	_, err := f.client.Conn().(*pgxpool.Pool).Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id UUID NOT NULL,
			aggregate_id UUID NOT NULL,
			value VARCHAR(20) NOT NULL,
			PRIMARY KEY (id)
		)`, FooReadModelTable))

	return err
}

func (f *FooReadModel) IsInitialized(ctx context.Context) (bool, error) {
	return f.client.Exists(ctx, FooReadModelTable)
}

func (f *FooReadModel) Reset(ctx context.Context) error {
	return f.client.Reset(ctx, FooReadModelTable)
}

func (f *FooReadModel) Delete(ctx context.Context) error {
	return f.client.Delete(ctx, FooReadModelTable)
}

func (f *FooReadModel) Stack(method string, args ...map[string]interface{}) {
	f.stack = append(f.stack, struct {
		method string
		args   []map[string]interface{}
	}{method: method, args: args})
}

func (f *FooReadModel) Persist(ctx context.Context) error {
	var err error
	for _, command := range f.stack {
		switch command.method {
		case "insert":
			err = f.client.Insert(ctx, FooReadModelTable, command.args[0])
			if err != nil {
				return err
			}
		case "remove":
			err = f.client.Remove(ctx, FooReadModelTable, command.args[0])
			if err != nil {
				return err
			}
		case "update":
			err = f.client.Update(ctx, FooReadModelTable, command.args[0], command.args[1])
			if err != nil {
				return err
			}
		}
	}

	f.stack = make([]struct {
		method string
		args   []map[string]interface{}
	}, 0)

	return err
}

func NewFooReadModel(client *pg.Client) *FooReadModel {
	return &FooReadModel{client: client}
}
```

### Create an ReadModel Projection

Create a new DB table "app_foo" and fill it with your event data

```go
typeRegistry := eventstore.NewTypeRegistry()
typeRegistry.RegisterAggregate(&FooAggregate{}, FooEvent{}, BarEvent{})

ps := pg.NewPersistenceStrategy(pool)
es := eventstore.NewEventStore(ps)
pm := pg.NewProjectionManager(pool)

client := pg.NewClient(pool)
rm := NewFooReadModel(client)

projector := eventstore.NewReadModelProjector("foo_read_model_projection", &rm, &es, &pm)
err = projector.
    FromStream(FooStream, []eventstore.MetadataMatch{}).
    Init(func() interface{} {
        return struct{}{}
    }).
    When(map[string]func(state interface{}, event eventstore.DomainEvent) interface{}{
        FooEventName: func(state interface{}, event eventstore.DomainEvent) interface{} {
            // persist a new entry to your ReadModel
            projector.ReadModel.Stack(
                "insert",
                map[string]interface{}{
                    "id":           event.UUID().String(),
                    "aggregate_id": event.AggregateID().String(),
                    "value":        event.Payload().(FooEvent).Foo,
                },
            )

            return state
        },
        BarEventName: func(state interface{}, event eventstore.DomainEvent) interface{} {
            projector.ReadModel.Stack(
                "insert",
                map[string]interface{}{
                    "id":           event.UUID().String(),
                    "aggregate_id": event.AggregateID().String(),
                    "value":        event.Payload().(BarEvent).Bar,
                },
            )

            return state
        },
    }).
    Run(ctx, false)
```

For more informations checkout the scripts in `example` or any `*_test.go` files

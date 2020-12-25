package eventstore

import "context"

// Client is a Helper to execute simple commands on a ReadModel
type Client interface {
	// Conn is the underlying Storage Connection like pgx.Pool for Postgres
	Conn() interface{}

	// Exists the given collection (table)
	Exists(ctx context.Context, collection string) (bool, error)
	// Delete the given collection (table)
	Delete(ctx context.Context, collection string) error
	// Reset (truncate) the given collection (table)
	Reset(ctx context.Context, collection string) error

	// Insert a new item into the collection, the map key represent the storage column
	Insert(ctx context.Context, collection string, values map[string]interface{}) error
	// Remove all items from the collection by the given identifiers, the map key represent the storage column
	Remove(ctx context.Context, collection string, identifiers map[string]interface{}) error
	// Update all matching items from the collection with the new values, the map key represent the storage column
	Update(ctx context.Context, collection string, values map[string]interface{}, identifiers map[string]interface{}) error
}

// ReadModel is a custom ReadModel of your DomainEvents and could be represented and peristed in many different forms
// For Example as DB Table in your Database or as cached files
// Example implementation in example/read_model.go
type ReadModel interface {
	// Init your ReadModel, for example create the DB Table
	Init(ctx context.Context) error
	// Check if your ReadModel was already initialized, for example if DB Table already exists
	IsInitialized(ctx context.Context) (bool, error)
	// Reset your ReadModel
	Reset(ctx context.Context) error
	// Delete your ReadModel
	Delete(ctx context.Context) error
	// Stack add a new command to you ReadModel
	Stack(method string, args ...map[string]interface{})
	// Persist the current State of your ReadModel, executes all stacked commands
	Persist(ctx context.Context) error
}

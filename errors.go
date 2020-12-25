package eventstore

import "fmt"

// StreamAlreadyExist is returned when you create an already existing EventStream
type StreamAlreadyExist struct {
	Stream string
}

func (e StreamAlreadyExist) Error() string {
	return fmt.Sprintf("Stream %s already exists", e.Stream)
}

// StreamNotFound is returned if you try to delete / reset or load a none existing EventStream
type StreamNotFound struct {
	Stream string
}

func (e StreamNotFound) Error() string {
	return fmt.Sprintf("Stream %s not found", e.Stream)
}

// ProjectionNotFound is returned if you try to delete / reset or load a none existing Projection
type ProjectionNotFound struct {
	Name string
}

func (e ProjectionNotFound) Error() string {
	return fmt.Sprintf("Projection %s not found", e.Name)
}

// ProjectorAlreadyInitialized panics if you call Init on an Projection twice
func ProjectorAlreadyInitialized() string {
	return "Projection already initialized"
}

// ProjectorFromWasAlreadyCalled panics if you call more then one of the available From Methods (FromStream, FromAll, FromStreams)
func ProjectorFromWasAlreadyCalled() string {
	return "Projection from was already called"
}

// ProjectorNoHandler panics if you run a Projection without defining a Handler
func ProjectorNoHandler() string {
	return "Projection has no handler"
}

// ProjectorStateNotInitialised panics if you don't call Init to initialise the ProjectionState
func ProjectorStateNotInitialised() string {
	return "Projection state not initialised"
}

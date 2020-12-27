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

// ProjectorAlreadyInitialized returns if you call Init on an Projection twice
type ProjectorAlreadyInitialized struct{}

func (e ProjectorAlreadyInitialized) Error() string {
	return "Projector already initialized"
}

// ProjectorFromWasAlreadyCalled returns if you call more then one of the available From Methods (FromStream, FromAll, FromStreams)
type ProjectorFromWasAlreadyCalled struct{}

func (e ProjectorFromWasAlreadyCalled) Error() string {
	return "Projector From Api was already called"
}

// ProjectorHandlerAlreadyDefined returns if you call more then one of the available When Methods (When, WhenAny)
type ProjectorHandlerAlreadyDefined struct{}

func (e ProjectorHandlerAlreadyDefined) Error() string {
	return "Projector When Api was already called"
}

// ProjectorHasNoHandler returns if you run a Projection without defining Handlertype ProjectorHandlerWasAlreadyDefined struct {}
type ProjectorHasNoHandler struct{}

func (e ProjectorHasNoHandler) Error() string {
	return "Projection has no handler"
}

// ProjectorStateNotInitialised returns if you don't call Init to initialise the ProjectionState
type ProjectorStateNotInitialised struct{}

func (e ProjectorStateNotInitialised) Error() string {
	return "Projection state not initialised"
}

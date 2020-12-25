package eventstore

import (
	"fmt"
	"reflect"
	"strings"
)

var methodHandlerPrefix = "When"

type HandlersCache map[reflect.Type]func(source interface{}, event interface{}, metadata map[string]interface{})

type TypeCache map[string]reflect.Type

// TypeRegistry register all existing Aggregates and Events for dynamic Type Assertions and Conversions
type TypeRegistry interface {
	GetHandlers(interface{}) HandlersCache
	GetTypeByName(string) (reflect.Type, bool)
	RegisterAggregate(aggregate interface{}, events ...interface{})
	RegisterEvents(events ...interface{})
	RegisterType(interface{})
}

type defaultTypeRegistry struct {
	HandlersDirectory map[reflect.Type]HandlersCache
	Types             TypeCache
}

var cachedRegistry *defaultTypeRegistry

// NewTypeRegistry constructs a new TypeRegistry
func NewTypeRegistry() TypeRegistry {
	return newTypeRegistry()
}

func newTypeRegistry() *defaultTypeRegistry {
	if cachedRegistry == nil {
		handlersDirectory := make(map[reflect.Type]HandlersCache, 0)
		types := make(TypeCache, 0)

		cachedRegistry = &defaultTypeRegistry{handlersDirectory, types}
	}

	return cachedRegistry
}

func (r *defaultTypeRegistry) GetHandlers(source interface{}) HandlersCache {
	sourceType := reflect.TypeOf(source)
	var handlers HandlersCache

	handlerChan := make(chan bool)
	quit := make(chan struct{})

	defer close(quit)
	go func(handlers *HandlersCache) {
		select {
		case handlerChan <- internalGetHandlers(r, sourceType, source, handlers):
		case <-quit:
			fmt.Println("quit")
		}
	}(&handlers)

	<-handlerChan
	return handlers
}

func internalGetHandlers(r *defaultTypeRegistry, sourceType reflect.Type, source interface{}, handlers *HandlersCache) bool {
	if value, ok := r.HandlersDirectory[sourceType]; ok {
		*handlers = value
	} else {
		*handlers = createHandlersCache(source)
		r.HandlersDirectory[sourceType] = *handlers
	}

	return true
}

func (r *defaultTypeRegistry) GetTypeByName(typeName string) (reflect.Type, bool) {
	typeValue, ok := r.Types[typeName]
	return typeValue, ok
}

func (r *defaultTypeRegistry) RegisterType(source interface{}) {
	rawType := reflect.TypeOf(source)
	r.Types[rawType.Name()] = rawType
}

func (r *defaultTypeRegistry) RegisterAggregate(aggregate interface{}, events ...interface{}) {
	r.RegisterType(aggregate)

	for _, event := range events {
		r.RegisterType(event)
	}
}

func (r *defaultTypeRegistry) RegisterEvents(events ...interface{}) {
	for _, event := range events {
		r.RegisterType(event)
	}
}

func createHandlersCache(source interface{}) HandlersCache {
	sourceType := reflect.TypeOf(source)
	handlers := make(HandlersCache)

	methodCount := sourceType.NumMethod()
	for i := 0; i < methodCount; i++ {
		method := sourceType.Method(i)
		if strings.HasPrefix(method.Name, methodHandlerPrefix) {
			if method.Type.NumIn() == 3 {
				eventType := method.Type.In(1)
				handler := func(source interface{}, event interface{}, metadata map[string]interface{}) {
					sourceValue := reflect.ValueOf(source)
					eventValue := reflect.ValueOf(event)
					metadataValue := reflect.ValueOf(metadata)

					method.Func.Call([]reflect.Value{sourceValue, eventValue, metadataValue})
				}

				handlers[eventType] = handler
			}
		}
	}

	return handlers
}

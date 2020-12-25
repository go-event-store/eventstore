package eventstore_test

import (
	"testing"

	eventstore "github.com/go-event-store/eventstore"
)

type FooEvent struct {
	Foo string
}

type BarEvent struct {
	Bar string
}

type FooAggregate struct {
	eventstore.BaseAggregate

	Foo string
}

func (f *FooAggregate) WhenFooEvent(e FooEvent, metadata map[string]interface{}) {
	f.Foo = e.Foo
}

func Test_TypeRegistry(t *testing.T) {
	t.Run("Register Events By Name", func(t *testing.T) {
		typeRegistry := eventstore.NewTypeRegistry()
		typeRegistry.RegisterEvents(FooEvent{}, BarEvent{})

		if _, ok := typeRegistry.GetTypeByName("FooEvent"); ok == false {
			t.Error("Should find Type for register Event")
		}

		if _, ok := typeRegistry.GetTypeByName("BarEvent"); ok == false {
			t.Error("Should find Type for register Event")
		}

		if _, ok := typeRegistry.GetTypeByName("BazEvent"); ok == true {
			t.Error("Can not find a Type for none existing event")
		}
	})

	t.Run("Get EventHandlers from Aggregate", func(t *testing.T) {
		typeRegistry := eventstore.NewTypeRegistry()
		typeRegistry.RegisterAggregate(FooAggregate{})
		typeRegistry.RegisterEvents(FooEvent{}, BarEvent{})

		if _, ok := typeRegistry.GetTypeByName("FooAggregate"); ok == false {
			t.Error("Should find Type for register Aggregate")
		}

		cache := typeRegistry.GetHandlers(&FooAggregate{})
		ty, ok := typeRegistry.GetTypeByName("FooEvent")

		if ok == false {
			t.Error("Should find Type for register Event")
		}

		if _, ok = cache[ty]; ok == false {
			t.Error("Should find Handlers for a registered Aggregate Event with Callback")
		}
	})
}

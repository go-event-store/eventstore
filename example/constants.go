package example

const (
	DatabaseURL = "postgres://user:password@localhost/event-store?sslmode=disable"

	FooStream    = "foo-stream"
	FooEventName = "FooEvent"
	BarEventName = "BarEvent"
)

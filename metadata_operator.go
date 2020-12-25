package eventstore

// Enum for possible Operators to filter EventStreams
type MetadataOperator string

const (
	EqualsOperator            MetadataOperator = "="
	NotEqualsOperator         MetadataOperator = "!="
	GreaterThanOperator       MetadataOperator = ">"
	GreaterThanEqualsOperator MetadataOperator = ">="
	InOperator                MetadataOperator = "in"
	NotInOperator             MetadataOperator = "nin"
	LowerThanOperator         MetadataOperator = "<"
	LowerThanEuqalsOperator   MetadataOperator = "<="
	RegexOperator             MetadataOperator = "regex"
)

// Enum for possible categories of fields to filter EventStreams
type FieldType string

const (
	MetadataField        FieldType = "metadata"
	MessagePropertyField FieldType = "message_property"
)

// MetadataMatcher alias of a List of MetadataMatch
type MetadataMatcher = []MetadataMatch

// MetadataMatch is a struct to filter an EventStream by Metadata or EventProperties
// Like EventName, AggregateID, Version, CreatedAt
type MetadataMatch struct {
	// Field name to filter
	// For MessagePropertyField possible values are "event_name", "created_at", "uuid"
	// For MetadataField its the name of the filtered metadata like "_aggregate_id"
	Field string
	// Value to filter
	Value interface{}
	// Operation to execute like EqualsOperator
	// Example for MetadataField and EqualsOperator checks if MetadataMatch.Value = event.metadata[MetadataMatch.Field]
	Operation MetadataOperator
	// FieldType to filter
	FieldType FieldType
}

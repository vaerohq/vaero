package execute

import "github.com/google/uuid"

type OpTask struct {
	Id       uuid.UUID
	Type     string                 `mapstructure:"type"` // source, sink, or tn
	Op       string                 `mapstructure:"op"`   // identify the source, sink, or tn
	Args     map[string]interface{} `mapstructure:"args"`
	Branches [][]OpTask             // only used for branches
	Secret   map[string]interface{} `mapstructure:"secret"` // only used with source and sink for retrieving secrets for params
}

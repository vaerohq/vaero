package execute

import "github.com/google/uuid"

type OpTask struct {
	Id       uuid.UUID
	Type     string                 `mapstructure:"type"`
	Op       string                 `mapstructure:"op"`
	Args     map[string]interface{} `mapstructure:"args"`
	Branches [][]OpTask
}

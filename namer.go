package postgres

import (
	"gorm.io/gorm/schema"
	"strings"
)

type Namer struct {
	schema.NamingStrategy
}

func (n Namer) IndexName(table, column string) (name string) {
	if !strings.HasPrefix(column, "idx_") {
		return n.NamingStrategy.IndexName(table, column)
	} else {
		return column
	}
}

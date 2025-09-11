package model

import (
	"gorm.io/gorm"
)

type KeyValue struct {
	gorm.Model
	Key   string `gorm:"uniqueIndex; not null"`
	Value string `gorm:"type:text"` // use string if plain text

}

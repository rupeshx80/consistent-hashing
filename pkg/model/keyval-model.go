package model

import (
	"gorm.io/gorm"
	"time"
)

type KeyValue struct {
	gorm.Model
	Key         string `gorm:"not null; index"`
	Value       string `gorm:"type:text"`
	VectorClock string `gorm:"type:text"`
	CreatedAt   time.Time
}

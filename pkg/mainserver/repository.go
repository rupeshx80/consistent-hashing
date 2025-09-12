package mainserver

import (
	"errors"

	"github.com/rupeshx80/consistent-hashing/pkg/db"
	"github.com/rupeshx80/consistent-hashing/pkg/model"
)

type KeyValueRepository struct{}

func NewKeyValueRepository() *KeyValueRepository {
	return &KeyValueRepository{}
}

func (r *KeyValueRepository) UpsertKeyValue(key, value string) error {

	var kv model.KeyValue
	
	result := db.RJ.Where("key = ?", key).First(&kv)
	if result.Error == nil {
		//updates value
		kv.Value = value
		return db.RJ.Save(&kv).Error
	}

	kv = model.KeyValue{Key: key, Value: value}
	return db.RJ.Create(&kv).Error
}

func (r *KeyValueRepository) GetKeyValue(key string) (*model.KeyValue, error) {
	var kv model.KeyValue
	result := db.RJ.Where("key = ?", key).First(&kv)

	if result.Error != nil {
		return nil, errors.New("key not found in DB")
	}
	return &kv, nil
}

func (r *KeyValueRepository) DeleteKeyValue(key string) error {

	return db.RJ.Where("key = ?", key).Delete(&model.KeyValue{}).Error
}

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


func (r *KeyValueRepository) PutVersion(key, value, vectorClock string) error{
	kv := model.KeyValue {
		Key: key,
		Value: value,
		VectorClock: vectorClock,
	}

	return db.RJ.Create(&kv).Error

}

func (r *KeyValueRepository) GetAllVersions(key string) ([]model.KeyValue, error) {
	var versions []model.KeyValue

	result := db.RJ.Where("key = ?", key).Order("created_at asc").Find(&versions)

	if result.Error != nil {
		return nil, result.Error
	}
	
	if len(versions) == 0 {
		return nil, errors.New("key not found in DB")
	}
	return versions, nil
}

func (r *KeyValueRepository) GetAllKeys() ([]string, error) {
    var keys []string
    result := db.RJ.Model(&model.KeyValue{}).Distinct("key").Pluck("key", &keys)
    if result.Error != nil {
        return nil, result.Error
    }
    return keys, nil
}


func (r *KeyValueRepository) DeleteAllVersions(key string) error {
	return db.RJ.Where("key = ?", key).Delete(&model.KeyValue{}).Error
}
package postgres

import (
	pq "gitee.com/opengauss/openGauss-connector-go-pq"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"testing"
	"time"
)

func TestParseConfig(t *testing.T) {
	config := &Config{
		Host:           "192.168.20.157",
		Port:           5432,
		Database:       "test",
		User:           "gj",
		Password:       "xbrother",
		TLSConfig:      nil,
		ConnectTimeout: 30,
		RuntimeParams: map[string]string{
			"rk":  "rv",
			"rk1": "rv1",
			"rk2": "rv2",
		},
		GssAPIParams: map[string]string{
			"rk":  "rv",
			"rk1": "rv1",
			"rk2": "rv2",
		},
		Fallbacks: []*pq.FallbackConfig{
			{
				Host:      "192.168.0.1",
				Port:      5001,
				TLSConfig: nil,
			},
			{
				Host:      "192.168.0.2",
				Port:      5002,
				TLSConfig: nil,
			},
			{
				Host:      "192.168.0.3",
				Port:      5003,
				TLSConfig: nil,
			},
		},
		TargetSessionAttrs:   "read-write",
		MinReadBufferSize:    300,
		CpBufferSize:         400,
		LogLevel:             2,
		AllowEncodingChanges: "AllowEncodingChanges",
	}
	t.Log(configTODSN(config))
}

func TestGorm(t *testing.T) {
	// CREATE SCHEMA my_schema;
	// GRANT USAGE ON SCHEMA my_schema TO user_persistence;
	// GRANT CREATE ON SCHEMA my_schema TO user_persistence;
	db, err := gorm.Open(New(Config{
		Host:     "192.168.20.157",
		Port:     5432,
		Database: "db_tpcc",
		User:     "user_persistence",
		Password: "1234@abc",
		RuntimeParams: map[string]string{
			"search_path": "my_schema",
		},
	}))
	db.Logger.LogMode(logger.Info)

	if err != nil {
		t.Fatal(err)
	}
	err = db.AutoMigrate(&Model{})
	if err != nil {
		t.Fatal(err)
	}
	result := db.Create(&Model{
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		DeletedAt: gorm.DeletedAt{},
	})
	if result.Error != nil {
		t.Fatal(result.Error)
	}
	t.Log("insert", result.RowsAffected)
	ms := make([]*Model, 0)
	err = db.Find(&ms).Error
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range ms {
		t.Log(m)
	}

}

// gorm.Model 的定义
type Model struct {
	ID        uint `gorm:"primaryKey;schema:my_schema"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

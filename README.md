# GORM PostgreSQL Driver

## Quick Start

```go
import (
  og "github.com/stitchcula/OpenGauss"
  "gorm.io/gorm"
)

// gitee.com/opengauss/openGauss-connector-go-pq
config := og.Config{
    Host:     "192.168.xx.xx",
    Port:     5432,
    Database: "db_tpcc",
    User:     "user_persistence",
    Password: "1234@abc",
    RuntimeParams: map[string]string{
    "search_path": "my_schema",
    },
}
db, err := gorm.Open(og.New(config), &gorm.Config{})
```

Checkout [https://gorm.io](https://gorm.io) for details.

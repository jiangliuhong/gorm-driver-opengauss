package postgres

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	pq "gitee.com/opengauss/openGauss-connector-go-pq"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Dialector struct {
	//*Config

	//   # Example DSN
	//   user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-ca
	//
	//   # Example URL
	//   postgres://jack:secret@pg.example.com:5432/mydb?sslmode=verify-ca
	DSN string

	*Config
}

//	type Config struct {
//		DriverName           string
//		DSN                  string
//		PreferSimpleProtocol bool
//		WithoutReturning     bool
//		Conn                 gorm.ConnPool
//	}

type Config struct {
	Host           string // host (e.g. localhost) or absolute path to unix domain socket directory (e.g. /private/tmp)
	Port           uint16
	Database       string
	User           string
	Password       string
	TLSConfig      *tls.Config // nil disables TLS
	ConnectTimeout time.Duration
	//DialFunc       pq.DialFunc   // e.g. net.Dialer.DialContext
	//LookupFunc     pq.LookupFunc // e.g. net.Resolver.LookupHost
	// BuildFrontend  BuildFrontendFunc
	RuntimeParams map[string]string // Run-time parameters to set on connection as session default values (e.g. search_path or application_name)
	GssAPIParams  map[string]string
	Fallbacks     []*pq.FallbackConfig

	TargetSessionAttrs string
	MinReadBufferSize  int64 // The minimum size of the internal read buffer. Default 8192.
	CpBufferSize       int64 // Defines the size of the copy buffer. Default 65535.

	// ValidateConnect is called during a connection attempt after a successful authentication with the PostgreSQL server.
	// It can be used to validate that the server is acceptable. If this returns an error the connection is closed and the next
	// fallback config is tried. This allows implementing high availability behavior such as libpq does with target_session_attrs.

	//ValidateConnect pq.ValidateConnectFunc

	// AfterConnect is called after ValidateConnect. It can be used to set up the connection (e.g. Set session variables
	// or prepare statements). If this returns an error the connection attempt fails.
	// AfterConnect AfterConnectFunc

	// OnNotice is a callback function called when a notice response is received.
	// OnNotice NoticeHandler

	// OnNotification is a callback function called when a notification from the LISTEN/NOTIFY system is received.
	// OnNotification NotificationHandler

	//CreatedByParseConfig bool // Used to enforce created by ParseConfig rule.

	//Logger   pq.Logger
	LogLevel pq.LogLevel

	// When using the V3 protocol the driver monitors changes in certain server configuration parameters
	// that should not be touched by end users.
	// The client_encoding setting is set by the driver and should not be altered.
	// If the driver detects a change it will abort the connection.
	// There is one legitimate exception to this behaviour though,
	// using the COPY command on a file residing on the server's filesystem.
	// The only means of specifying the encoding of this file is by altering the client_encoding setting.
	// The JDBC team considers this a failing of the COPY command and hopes to provide an alternate means of specifying
	// the encoding in the future, but for now there is this URL parameter.
	// Enable this only if you need to override the client encoding when doing a copy.
	AllowEncodingChanges string
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{DSN: dsn}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (dialector Dialector) Name() string {
	return "opengauss"
}

//var timeZoneMatcher = regexp.MustCompile("(time_zone|TimeZone)=(.*?)($|&| )")

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {
	db.NamingStrategy = Namer{}
	// register callbacks
	//if !dialector.WithoutReturning {
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		CreateClauses: []string{"INSERT", "VALUES", "ON CONFLICT", "RETURNING"},
		UpdateClauses: []string{"UPDATE", "SET", "WHERE", "RETURNING"},
		DeleteClauses: []string{"DELETE", "FROM", "WHERE", "RETURNING"},
	})
	//}

	//if dialector.Conn != nil {
	//	db.ConnPool = dialector.Conn
	//} else if dialector.DriverName != "" {
	//	db.ConnPool, err = sql.Open(dialector.DriverName, dialector.Config.DSN)
	//} else {
	//	var config *pgx.ConnConfig
	//
	//	config, err = pgx.ParseConfig(dialector.Config.DSN)
	//	if err != nil {
	//		return
	//	}
	//	if dialector.Config.PreferSimpleProtocol {
	//		config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	//	}
	//	result := timeZoneMatcher.FindStringSubmatch(dialector.Config.DSN)
	//	if len(result) > 2 {
	//		config.RuntimeParams["timezone"] = result[2]
	//	}
	//	db.ConnPool = stdlib.OpenDB(*config)
	//}
	if dialector.DSN == "" {
		dialector.DSN = configTODSN(dialector.Config)
	}
	var config *pq.Config
	config, err = pq.ParseConfig(dialector.DSN)
	if err != nil {
		return err
	}
	connector, err := pq.NewConnectorConfig(config)
	if err != nil {
		return err
	}
	db.ConnPool = sql.OpenDB(connector)

	return
}

// # Example DSN
// user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-ca
func configTODSN(config *Config) string {
	s := strings.Builder{}
	builder := func(k, v string) {
		if v == "" || v == "0" {
			return
		}
		s.WriteString(k)
		s.WriteString("=")
		s.WriteString(v)
		s.WriteString(" ")
	}

	// Fallbacks     []*pq.FallbackConfig
	host, port := config.Host, strconv.Itoa(int(config.Port))
	for _, fallback := range config.Fallbacks {
		host += "," + fallback.Host
		port += "," + strconv.Itoa(int(fallback.Port))
	}
	builder(paramHost, host)
	builder(paramPort, port)
	builder("dbname", config.Database)
	builder(paramUser, config.User)
	builder(paramPassword, config.Password)
	if config.ConnectTimeout != 0 {
		builder(paramConnectTimeout, fmt.Sprintf("%d", int(config.ConnectTimeout.Seconds()))) // time.Duration(timeout) * time.Second
	}

	builder(paramAllowEncodingChanges, config.AllowEncodingChanges)
	builder(paramCpBufferSize, strconv.FormatInt(config.CpBufferSize, 10))
	builder(paramMinReadBufferSize, strconv.FormatInt(config.MinReadBufferSize, 10))
	builder(paramTargetSessionAttrs, config.TargetSessionAttrs)

	for k, v := range config.RuntimeParams {
		builder(k, v)
	}
	for k, v := range config.GssAPIParams {
		builder(k, v)
	}

	switch config.LogLevel {
	case pq.LogLevelTrace:
		builder(paramLoggerLevel, "trace")
	case pq.LogLevelDebug:
		builder(paramLoggerLevel, "debug")
	case pq.LogLevelInfo:
		builder(paramLoggerLevel, "info")
	case pq.LogLevelWarn:
		builder(paramLoggerLevel, "warn")
	case pq.LogLevelError:
		builder(paramLoggerLevel, "error")
	case pq.LogLevelNone:
		builder(paramLoggerLevel, "none")
	}

	// TODO: tls undone.
	if config.TLSConfig == nil {
		builder(paramSSLMode, "disable")
	} else if config.TLSConfig.InsecureSkipVerify {
		builder(paramSSLMode, "allow")
	} else if config.TLSConfig.VerifyPeerCertificate != nil {
		builder(paramSSLMode, "verify-ca")
	} else if config.TLSConfig.ServerName != "" {
		builder(paramSSLMode, "verify-full")
	}

	return s.String()
}

const (
	paramClientEncoding              = "client_encoding"
	paramAllowEncodingChanges        = "allow_encoding_changes"
	paramLoggerLevel                 = "loggerLevel"
	paramCpBufferSize                = "cp_buffer_size"
	paramMinReadBufferSize           = "min_read_buffer_size"
	paramTargetSessionAttrs          = "target_session_attrs"
	paramHost                        = "host"
	paramPort                        = "port"
	paramDatabase                    = "database"
	paramUser                        = "user"
	paramPassword                    = "password"
	paramPassFile                    = "passfile"
	paramConnectTimeout              = "connect_timeout"
	paramSSLMode                     = "sslmode"
	paramSSLKey                      = "sslkey"
	paramSSLCert                     = "sslcert"
	paramSSLRootCert                 = "sslrootcert"
	paramSSLinLine                   = "sslinline"
	paramSSLPassword                 = "sslpassword"
	paramService                     = "service"
	paramKrbSrvName                  = "krbsrvname"
	paramKrbSpn                      = "krbspn"
	paramServiceFile                 = "servicefile"
	paramDisablePreparedBinaryResult = "disable_prepared_binary_result"
	paramApplicationName             = "application_name"
)

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:                          db,
		Dialector:                   dialector,
		CreateIndexAfterCreateTable: true,
	}}}
}

func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('$')
	writer.WriteString(strconv.Itoa(len(stmt.Vars)))
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	var (
		underQuoted, selfQuoted bool
		continuousBacktick      int8
		shiftDelimiter          int8
	)

	for _, v := range []byte(str) {
		switch v {
		case '"':
			continuousBacktick++
			if continuousBacktick == 2 {
				writer.WriteString(`""`)
				continuousBacktick = 0
			}
		case '.':
			if continuousBacktick > 0 || !selfQuoted {
				shiftDelimiter = 0
				underQuoted = false
				continuousBacktick = 0
				writer.WriteByte('"')
			}
			writer.WriteByte(v)
			continue
		default:
			if shiftDelimiter-continuousBacktick <= 0 && !underQuoted {
				writer.WriteByte('"')
				underQuoted = true
				if selfQuoted = continuousBacktick > 0; selfQuoted {
					continuousBacktick -= 1
				}
			}

			for ; continuousBacktick > 0; continuousBacktick -= 1 {
				writer.WriteString(`""`)
			}

			writer.WriteByte(v)
		}
		shiftDelimiter++
	}

	if continuousBacktick > 0 && !selfQuoted {
		writer.WriteString(`""`)
	}
	writer.WriteByte('"')
}

var numericPlaceholder = regexp.MustCompile(`\$(\d+)`)

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	fmt.Println(logger.ExplainSQL(sql, numericPlaceholder, `'`, vars...))
	return logger.ExplainSQL(sql, numericPlaceholder, `'`, vars...)
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "boolean"
	case schema.Int, schema.Uint:
		size := field.Size
		if field.DataType == schema.Uint {
			size++
		}
		if field.AutoIncrement {
			switch {
			case size <= 16:
				return "smallserial"
			case size <= 32:
				return "serial"
			default:
				return "bigserial"
			}
		} else {
			switch {
			case size <= 16:
				return "smallint"
			case size <= 32:
				return "integer"
			default:
				return "bigint"
			}
		}
	case schema.Float:
		if field.Precision > 0 {
			if field.Scale > 0 {
				return fmt.Sprintf("numeric(%d, %d)", field.Precision, field.Scale)
			}
			return fmt.Sprintf("numeric(%d)", field.Precision)
		}
		return "decimal"
	case schema.String:
		if field.Size > 0 {
			return fmt.Sprintf("varchar(%d)", field.Size)
		}
		return "text"
	case schema.Time:
		if field.Precision > 0 {
			return fmt.Sprintf("timestamptz(%d)", field.Precision)
		}
		return "timestamptz"
	case schema.Bytes:
		return "bytea"
	default:
		return dialector.getSchemaCustomType(field)
	}
}

func (dialector Dialector) getSchemaCustomType(field *schema.Field) string {
	sqlType := string(field.DataType)

	if field.AutoIncrement && !strings.Contains(strings.ToLower(sqlType), "serial") {
		size := field.Size
		if field.GORMDataType == schema.Uint {
			size++
		}
		switch {
		case size <= 16:
			sqlType = "smallserial"
		case size <= 32:
			sqlType = "serial"
		default:
			sqlType = "bigserial"
		}
	}

	return sqlType
}

func (dialector Dialector) SavePoint(tx *gorm.DB, name string) error {
	tx.Exec("SAVEPOINT " + name)
	return nil
}

func (dialector Dialector) RollbackTo(tx *gorm.DB, name string) error {
	tx.Exec("ROLLBACK TO SAVEPOINT " + name)
	return nil
}

func getSerialDatabaseType(s string) (dbType string, ok bool) {
	switch s {
	case "smallserial":
		return "smallint", true
	case "serial":
		return "integer", true
	case "bigserial":
		return "bigint", true
	default:
		return "", false
	}
}

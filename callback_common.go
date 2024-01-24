package postgres

import (
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"reflect"
	"strings"
)

// ConvertMysqlSql 兼容mysql的sql
// 将 ` 转为 "
func ConvertMysqlSql(sql string) string {
	// 替换 `` 符号
	sql = strings.ReplaceAll(sql, "`", "\"")
	return sql
}

func BuildQuerySQL(db *gorm.DB) {
	buildQuerySQL(db)
	sql := db.Statement.SQL.String()
	sql = ConvertMysqlSql(sql)
	db.Statement.SQL.Reset()
	db.Statement.SQL.WriteString(sql)
}

func buildQuerySQL(db *gorm.DB) {
	if db.Statement.Schema != nil {
		for _, c := range db.Statement.Schema.QueryClauses {
			db.Statement.AddClause(c)
		}
	}

	if db.Statement.SQL.Len() == 0 {
		db.Statement.SQL.Grow(100)
		clauseSelect := clause.Select{Distinct: db.Statement.Distinct}

		if db.Statement.ReflectValue.Kind() == reflect.Struct && db.Statement.ReflectValue.Type() == db.Statement.Schema.ModelType {
			var conds []clause.Expression
			for _, primaryField := range db.Statement.Schema.PrimaryFields {
				if v, isZero := primaryField.ValueOf(db.Statement.Context, db.Statement.ReflectValue); !isZero {
					conds = append(conds, clause.Eq{Column: clause.Column{Table: db.Statement.Table, Name: primaryField.DBName}, Value: v})
				}
			}

			if len(conds) > 0 {
				db.Statement.AddClause(clause.Where{Exprs: conds})
			}
		}

		if len(db.Statement.Selects) > 0 {
			clauseSelect.Columns = make([]clause.Column, len(db.Statement.Selects))
			for idx, name := range db.Statement.Selects {
				if db.Statement.Schema == nil {
					clauseSelect.Columns[idx] = clause.Column{Name: name, Raw: true}
				} else if f := db.Statement.Schema.LookUpField(name); f != nil {
					clauseSelect.Columns[idx] = clause.Column{Name: f.DBName}
				} else {
					clauseSelect.Columns[idx] = clause.Column{Name: name, Raw: true}
				}
			}
		} else if db.Statement.Schema != nil && len(db.Statement.Omits) > 0 {
			selectColumns, _ := db.Statement.SelectAndOmitColumns(false, false)
			clauseSelect.Columns = make([]clause.Column, 0, len(db.Statement.Schema.DBNames))
			for _, dbName := range db.Statement.Schema.DBNames {
				if v, ok := selectColumns[dbName]; (ok && v) || !ok {
					clauseSelect.Columns = append(clauseSelect.Columns, newColumn(db.Statement.Table, dbName))
				}
			}
		} else if db.Statement.Schema != nil && db.Statement.ReflectValue.IsValid() {
			queryFields := db.QueryFields
			if !queryFields {
				switch db.Statement.ReflectValue.Kind() {
				case reflect.Struct:
					queryFields = db.Statement.ReflectValue.Type() != db.Statement.Schema.ModelType
				case reflect.Slice:
					queryFields = db.Statement.ReflectValue.Type().Elem() != db.Statement.Schema.ModelType
				}
			}

			if queryFields {
				stmt := gorm.Statement{DB: db}
				// smaller struct
				if err := stmt.Parse(db.Statement.Dest); err == nil && (db.QueryFields || stmt.Schema.ModelType != db.Statement.Schema.ModelType) {
					clauseSelect.Columns = make([]clause.Column, len(stmt.Schema.DBNames))

					for idx, dbName := range stmt.Schema.DBNames {
						clauseSelect.Columns[idx] = newColumn(db.Statement.Table, dbName)
					}
				}
			}
		}

		// inline joins
		fromClause := clause.From{}
		if v, ok := db.Statement.Clauses["FROM"].Expression.(clause.From); ok {
			fromClause = v
		}

		if len(db.Statement.Joins) != 0 || len(fromClause.Joins) != 0 {
			if len(db.Statement.Selects) == 0 && len(db.Statement.Omits) == 0 && db.Statement.Schema != nil {
				clauseSelect.Columns = make([]clause.Column, len(db.Statement.Schema.DBNames))
				for idx, dbName := range db.Statement.Schema.DBNames {
					clauseSelect.Columns[idx] = newColumn(db.Statement.Table, dbName)
				}
			}

			for _, join := range db.Statement.Joins {
				if db.Statement.Schema == nil {
					fromClause.Joins = append(fromClause.Joins, clause.Join{
						Expression: clause.NamedExpr{SQL: join.Name, Vars: join.Conds},
					})
				} else if relation, ok := db.Statement.Schema.Relationships.Relations[join.Name]; ok {
					tableAliasName := relation.Name

					for _, s := range relation.FieldSchema.DBNames {
						clauseSelect.Columns = append(clauseSelect.Columns, clause.Column{
							Table: tableAliasName,
							Name:  s,
							Alias: tableAliasName + "__" + s,
						})
					}

					exprs := make([]clause.Expression, len(relation.References))
					for idx, ref := range relation.References {
						if ref.OwnPrimaryKey {
							exprs[idx] = clause.Eq{
								Column: clause.Column{Table: clause.CurrentTable, Name: ref.PrimaryKey.DBName},
								Value:  clause.Column{Table: tableAliasName, Name: ref.ForeignKey.DBName},
							}
						} else {
							if ref.PrimaryValue == "" {
								exprs[idx] = clause.Eq{
									Column: clause.Column{Table: clause.CurrentTable, Name: ref.ForeignKey.DBName},
									Value:  clause.Column{Table: tableAliasName, Name: ref.PrimaryKey.DBName},
								}
							} else {
								exprs[idx] = clause.Eq{
									Column: clause.Column{Table: tableAliasName, Name: ref.ForeignKey.DBName},
									Value:  ref.PrimaryValue,
								}
							}
						}
					}

					{
						onStmt := gorm.Statement{Table: tableAliasName, DB: db, Clauses: map[string]clause.Clause{}}
						for _, c := range relation.FieldSchema.QueryClauses {
							onStmt.AddClause(c)
						}

						if join.On != nil {
							onStmt.AddClause(join.On)
						}

						if cs, ok := onStmt.Clauses["WHERE"]; ok {
							if where, ok := cs.Expression.(clause.Where); ok {
								where.Build(&onStmt)

								if onSQL := onStmt.SQL.String(); onSQL != "" {
									vars := onStmt.Vars
									for idx, v := range vars {
										bindvar := strings.Builder{}
										onStmt.Vars = vars[0 : idx+1]
										db.Dialector.BindVarTo(&bindvar, &onStmt, v)
										onSQL = strings.Replace(onSQL, bindvar.String(), "?", 1)
									}

									exprs = append(exprs, clause.Expr{SQL: onSQL, Vars: vars})
								}
							}
						}
					}

					fromClause.Joins = append(fromClause.Joins, clause.Join{
						Type:  clause.LeftJoin,
						Table: clause.Table{Name: relation.FieldSchema.Table, Alias: tableAliasName},
						ON:    clause.Where{Exprs: exprs},
					})
				} else {
					fromClause.Joins = append(fromClause.Joins, clause.Join{
						Expression: clause.NamedExpr{SQL: join.Name, Vars: join.Conds},
					})
				}
			}

			db.Statement.AddClause(fromClause)
			db.Statement.Joins = nil
		} else {
			db.Statement.AddClauseIfNotExists(clause.From{})
		}

		db.Statement.AddClauseIfNotExists(clauseSelect)

		db.Statement.Build(db.Statement.BuildClauses...)
	}
}

func newColumn(table, name string) clause.Column {
	cc := clause.Column{Table: table, Name: name}
	//if IsReservedWord(name) {
	//	// 针对关键字进行as
	//	cc.Alias = fmt.Sprintf(`"%s"`, name)
	//}
	return cc
}

func checkMissingWhereConditions(db *gorm.DB) {
	if !db.AllowGlobalUpdate && db.Error == nil {
		where, withCondition := db.Statement.Clauses["WHERE"]
		if withCondition {
			if _, withSoftDelete := db.Statement.Clauses["soft_delete_enabled"]; withSoftDelete {
				whereClause, _ := where.Expression.(clause.Where)
				withCondition = len(whereClause.Exprs) > 1
			}
		}
		if !withCondition {
			db.AddError(gorm.ErrMissingWhereClause)
		}
		return
	}
}

func hasReturning(tx *gorm.DB, supportReturning bool) (bool, gorm.ScanMode) {
	if supportReturning {
		if c, ok := tx.Statement.Clauses["RETURNING"]; ok {
			returning, _ := c.Expression.(clause.Returning)
			if len(returning.Columns) == 0 || (len(returning.Columns) == 1 && returning.Columns[0].Name == "*") {
				return true, 0
			}
			return true, gorm.ScanUpdate
		}
	}
	return false, 0
}

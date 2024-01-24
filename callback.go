package postgres

import (
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
	"gorm.io/gorm/utils"
	"reflect"
)

func Query(db *gorm.DB) {
	if db.Error == nil {
		BuildQuerySQL(db)

		if !db.DryRun && db.Error == nil {
			rows, err := db.Statement.ConnPool.QueryContext(db.Statement.Context, db.Statement.SQL.String(), db.Statement.Vars...)
			if err != nil {
				db.AddError(err)
				return
			}
			defer func() {
				db.AddError(rows.Close())
			}()
			gorm.Scan(rows, db, 0)
		}
	}
}

func RowQuery(db *gorm.DB) {
	if db.Error == nil {
		BuildQuerySQL(db)
		if db.DryRun {
			return
		}

		if isRows, ok := db.Get("rows"); ok && isRows.(bool) {
			db.Statement.Settings.Delete("rows")
			db.Statement.Dest, db.Error = db.Statement.ConnPool.QueryContext(db.Statement.Context, db.Statement.SQL.String(), db.Statement.Vars...)
		} else {
			db.Statement.Dest = db.Statement.ConnPool.QueryRowContext(db.Statement.Context, db.Statement.SQL.String(), db.Statement.Vars...)
		}

		db.RowsAffected = -1
	}
}

func Update(config *callbacks.Config) func(db *gorm.DB) {
	supportReturning := utils.Contains(config.UpdateClauses, "RETURNING")

	return func(db *gorm.DB) {
		if db.Error != nil {
			return
		}

		if db.Statement.Schema != nil {
			for _, c := range db.Statement.Schema.UpdateClauses {
				db.Statement.AddClause(c)
			}
		}

		if db.Statement.SQL.Len() == 0 {
			db.Statement.SQL.Grow(180)
			db.Statement.AddClauseIfNotExists(clause.Update{})
			if set := callbacks.ConvertToAssignments(db.Statement); len(set) != 0 {
				db.Statement.AddClause(set)
			} else if _, ok := db.Statement.Clauses["SET"]; !ok {
				return
			}

			db.Statement.Build(db.Statement.BuildClauses...)
		}

		checkMissingWhereConditions(db)

		if !db.DryRun && db.Error == nil {
			if ok, mode := hasReturning(db, supportReturning); ok {
				sql := ConvertMysqlSql(db.Statement.SQL.String())
				if rows, err := db.Statement.ConnPool.QueryContext(db.Statement.Context, sql, db.Statement.Vars...); db.AddError(err) == nil {
					dest := db.Statement.Dest
					db.Statement.Dest = db.Statement.ReflectValue.Addr().Interface()
					gorm.Scan(rows, db, mode)
					db.Statement.Dest = dest
					db.AddError(rows.Close())
				}
			} else {
				sql := ConvertMysqlSql(db.Statement.SQL.String())
				result, err := db.Statement.ConnPool.ExecContext(db.Statement.Context, sql, db.Statement.Vars...)

				if db.AddError(err) == nil {
					db.RowsAffected, _ = result.RowsAffected()
				}
			}
		}
	}
}

func Delete(config *callbacks.Config) func(db *gorm.DB) {
	supportReturning := utils.Contains(config.DeleteClauses, "RETURNING")

	return func(db *gorm.DB) {
		if db.Error != nil {
			return
		}

		if db.Statement.Schema != nil {
			for _, c := range db.Statement.Schema.DeleteClauses {
				db.Statement.AddClause(c)
			}
		}

		if db.Statement.SQL.Len() == 0 {
			db.Statement.SQL.Grow(100)
			db.Statement.AddClauseIfNotExists(clause.Delete{})

			if db.Statement.Schema != nil {
				_, queryValues := schema.GetIdentityFieldValuesMap(db.Statement.Context, db.Statement.ReflectValue, db.Statement.Schema.PrimaryFields)
				column, values := schema.ToQueryValues(db.Statement.Table, db.Statement.Schema.PrimaryFieldDBNames, queryValues)

				if len(values) > 0 {
					db.Statement.AddClause(clause.Where{Exprs: []clause.Expression{clause.IN{Column: column, Values: values}}})
				}

				if db.Statement.ReflectValue.CanAddr() && db.Statement.Dest != db.Statement.Model && db.Statement.Model != nil {
					_, queryValues = schema.GetIdentityFieldValuesMap(db.Statement.Context, reflect.ValueOf(db.Statement.Model), db.Statement.Schema.PrimaryFields)
					column, values = schema.ToQueryValues(db.Statement.Table, db.Statement.Schema.PrimaryFieldDBNames, queryValues)

					if len(values) > 0 {
						db.Statement.AddClause(clause.Where{Exprs: []clause.Expression{clause.IN{Column: column, Values: values}}})
					}
				}
			}

			db.Statement.AddClauseIfNotExists(clause.From{})

			db.Statement.Build(db.Statement.BuildClauses...)
		}

		checkMissingWhereConditions(db)

		if !db.DryRun && db.Error == nil {
			ok, mode := hasReturning(db, supportReturning)
			if !ok {
				sql := ConvertMysqlSql(db.Statement.SQL.String())
				result, err := db.Statement.ConnPool.ExecContext(db.Statement.Context, sql, db.Statement.Vars...)
				if db.AddError(err) == nil {
					db.RowsAffected, _ = result.RowsAffected()
				}

				return
			}

			sql := ConvertMysqlSql(db.Statement.SQL.String())
			if rows, err := db.Statement.ConnPool.QueryContext(db.Statement.Context, sql, db.Statement.Vars...); db.AddError(err) == nil {
				gorm.Scan(rows, db, mode)
				db.AddError(rows.Close())
			}
		}
	}
}

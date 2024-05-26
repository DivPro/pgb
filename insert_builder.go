package pgb

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type (
	// SQLValue is a SQL expression intended for use where a value would normally be expected. It is not escaped or sanitized.
	SQLValue string
	// Excluded is a column identifier that will be prefixed with EXCLUDED
	Excluded string
)

type RowMapper[T any] func(v T) map[string]any

type InsertBuilder[T any] struct {
	tableName  string
	values     []T
	valueFn    RowMapper[T]
	constraint string
	do         map[string]any
	returning  []string

	buildOnce sync.Once
	sql       string
	args      []any
}

func NewInsertBuilder[T any](tableName string, values []T, valueFn RowMapper[T]) *InsertBuilder[T] {
	return &InsertBuilder[T]{
		tableName: tableName,
		values:    values,
		valueFn:   valueFn,
	}
}

func (b *InsertBuilder[T]) OnConflictDoNothing(constraint string) *InsertBuilder[T] {
	b.constraint = constraint

	return b
}

func (b *InsertBuilder[T]) OnConflictDoUpdate(constraint string, do map[string]any) *InsertBuilder[T] {
	b.constraint = constraint
	b.do = do

	return b
}

func (b *InsertBuilder[T]) Returning(returning ...string) *InsertBuilder[T] {
	b.returning = returning

	return b
}

func (b *InsertBuilder[T]) Build() (string, []any) {
	b.build()

	return b.sql, b.args
}

func (b *InsertBuilder[T]) build() *InsertBuilder[T] {
	b.buildOnce.Do(func() {
		sql := &strings.Builder{}

		sql.WriteString("INSERT INTO ")
		sql.WriteString(ident(b.tableName).Sanitize())
		sql.WriteString(" (")

		rows := make([]map[string]any, len(b.values))
		for i, value := range b.values {
			rows[i] = b.valueFn(value)
		}

		keys := make([]string, 0, len(rows[0]))
		for k := range rows[0] {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for i, k := range keys {
			if i > 0 {
				sql.WriteString(", ")
			}
			sanitizedKey := ident(k).Sanitize()
			sql.WriteString(sanitizedKey)
		}

		b.args = make([]any, 0, len(keys))
		for i, values := range rows {
			if i == 0 {
				sql.WriteString(") VALUES (")
			} else {
				sql.WriteString("), (")
			}

			for j, key := range keys {
				if j > 0 {
					sql.WriteString(", ")
				}
				if SQLValue, ok := values[key].(SQLValue); ok {
					sql.WriteString(string(SQLValue))
				} else {
					b.args = append(b.args, values[key])
					sql.WriteByte('$')
					sql.WriteString(strconv.FormatInt(int64(len(b.args)), 10))
				}
			}
		}

		sql.WriteString(")")
		if b.constraint != "" {
			sql.WriteString(" ON CONFLICT ON CONSTRAINT ")
			sql.WriteString(ident(b.constraint).Sanitize())
			sql.WriteString(" DO ")
			if len(b.do) == 0 {
				sql.WriteString("NOTHING")
			} else {
				sql.WriteString("UPDATE SET ")
				sep := false
				keys := make([]string, 0, len(b.do))
				for k := range b.do {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, k := range keys {
					if sep {
						sql.WriteString(", ")
					}
					sep = true
					sql.WriteString(ident(k).Sanitize())
					sql.WriteString(" = ")
					if b.do[k] == nil {
						sql.WriteString("NULL")
						continue
					}
					switch v := b.do[k].(type) {
					case Excluded:
						sql.WriteString("EXCLUDED.")
						sql.WriteString(ident(v).Sanitize())
					case SQLValue:
						sql.WriteString(string(v))
					case string:
						sql.WriteString("'")
						sql.WriteString(v)
						sql.WriteString("'")
					default:
						sql.WriteString(fmt.Sprint(v))
					}
				}
			}
		}

		if len(b.returning) != 0 {
			sql.WriteString(" RETURNING ")
			for i, s := range b.returning {
				if i > 0 {
					sql.WriteString(", ")
				}
				sql.WriteString(ident(s).Sanitize())
			}
		}
		b.sql = sql.String()
	})

	return b
}

func (b *InsertBuilder[T]) RawSql() string {
	b.build()

	sql := b.sql
	for i, v := range b.args {
		var val string
		switch v.(type) {
		case string:
			val = fmt.Sprintf("'%v'", v)
		default:
			val = fmt.Sprint(v)
		}
		sql = strings.ReplaceAll(sql, fmt.Sprintf("$%d", i+1), val)
	}

	return sql
}

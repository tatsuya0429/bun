package googlesqldialect

import (
	"encoding/json"
	"reflect"

	"github.com/uptrace/bun/dialect/sqltype"
	"github.com/uptrace/bun/schema"
)

const (
	googlesqlTypeTimestamp = "TIMESTAMP"
	googlesqlTypeDate      = "DATE"
	googlesqlTypeBytes     = "BYTES"
	googlesqlTypeString    = "STRING"
	googlesqlTypeStruct    = "STRUCT"
	googlesqlTypeInt64     = "INT64"
	googlesqlTypeFloat64   = "FLOAT64"
	googlesqlTypeBool      = "BOOL"
	googlesqlTypeNumeric   = "NUMERIC"
	googlesqlTypeJson      = "JSON"
)

var (
	jsonRawMessageType = reflect.TypeOf((*json.RawMessage)(nil)).Elem()
)

func fieldSQLType(field *schema.Field) string {
	if field.UserSQLType != "" {
		return field.UserSQLType
	}

	if field.Tag.HasOption("array") {
		switch field.IndirectType.Kind() {
		case reflect.Slice, reflect.Array:
			sqlType := sqlType(field.IndirectType.Elem())
			return sqlType + "[]"
		}
	}
	return sqlType(field.IndirectType)
}

func sqlType(typ reflect.Type) string {
	switch typ {
	case jsonRawMessageType:
		return googlesqlTypeJson
	}
	sqlType := schema.DiscoverSQLType(typ)
	switch sqlType {
	case sqltype.Boolean:
		return googlesqlTypeBool
	case sqltype.Blob:
		return googlesqlTypeBytes
	case sqltype.BigInt, sqltype.SmallInt, sqltype.Integer:
		return googlesqlTypeInt64
	case sqltype.DoublePrecision, sqltype.Real:
		return googlesqlTypeFloat64
	case sqltype.VarChar:
		return googlesqlTypeString
	case sqltype.JSON:
		return googlesqlTypeString
	case sqltype.JSONB:
		return googlesqlTypeBytes
	}
	switch typ.Kind() {
	case reflect.Map, reflect.Struct:
		return googlesqlTypeJson
	case reflect.Array, reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 {
			return googlesqlTypeBytes
		}
		return googlesqlTypeJson
	}
	return sqlType
}

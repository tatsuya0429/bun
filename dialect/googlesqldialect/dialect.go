package googlesqldialect

import (
	"database/sql"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
	"github.com/uptrace/bun/dialect/feature"
	"github.com/uptrace/bun/schema"
)

func init() {
	if Version() != bun.Version() {
		panic(fmt.Errorf("googlesqldialect and Bun must have the same version: v%s != v%s",
			Version(), bun.Version()))
	}
}

type Dialect struct {
	schema.BaseDialect

	tables   *schema.Tables
	features feature.Feature
}

func New() *Dialect {
	d := new(Dialect)
	d.tables = schema.NewTables(d)
	d.features = feature.CTE |
		feature.WithValues |
		feature.Returning |
		feature.InsertReturning |
		feature.InsertTableAlias |
		feature.UpdateTableAlias |
		feature.DeleteTableAlias |
		feature.InsertOnConflict |
		feature.TableNotExists |
		feature.SelectExists |
		feature.CompositeIn
	return d
}

func (d *Dialect) Init(*sql.DB) {}

func (d *Dialect) Name() dialect.Name {
	return dialect.GoogleSQL
}

func (d *Dialect) Features() feature.Feature {
	return d.features
}

func (d *Dialect) Tables() *schema.Tables {
	return d.tables
}

func (d *Dialect) OnTable(table *schema.Table) {
	for _, field := range table.FieldMap {
		d.onField(field)
	}
}

func (d *Dialect) onField(field *schema.Field) {
	field.DiscoveredSQLType = fieldSQLType(field)
}

func (d *Dialect) IdentQuote() byte {
	return '"'
}

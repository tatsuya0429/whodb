package spanner

import (
	"context"
	"errors"

	spannerdb "cloud.google.com/go/spanner"
	"github.com/clidey/whodb/core/src/engine"
	"google.golang.org/api/iterator"
)

type SpannerPlugin struct{}

func (p *SpannerPlugin) IsAvailable(config *engine.PluginConfig) bool {
	db, err := DB(config)
	if err != nil {
		return false
	}
	defer db.Close()
	return true
}

func (p *SpannerPlugin) GetDatabases(config *engine.PluginConfig) ([]string, error) {
	return nil, errors.ErrUnsupported
}

func (p *SpannerPlugin) GetSchema(config *engine.PluginConfig) ([]string, error) {
	ctx := context.Background()
	db, err := DB(config)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	schemaNames := []string{}

	statement := spannerdb.Statement{
		SQL: "SELECT schema_name FROM information_schema.schemata",
	}

	iter := db.ReadOnlyTransaction().Query(ctx, statement)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var schemaName string
		if err := row.Columns(&schemaName); err != nil {
			return nil, err
		}
		schemaNames = append(schemaNames, schemaName)
	}

	return schemaNames, nil
}

func (p *SpannerPlugin) GetStorageUnits(config *engine.PluginConfig, schema string) ([]engine.StorageUnit, error) {
	ctx := context.Background()
	db, err := DB(config)
	if err != nil {
		return nil, err
	}

	defer db.Close()

	storageUnits := []engine.StorageUnit{}

	stmt := spannerdb.Statement{
		SQL: `
		SELECT
			t.TABLE_NAME AS table_name,
			t.TABLE_TYPE AS table_type
		FROM
			INFORMATION_SCHEMA.TABLES AS t
		WHERE
			t.TABLE_SCHEMA = @schema
	`,
		Params: map[string]interface{}{
			"schema": schema,
		},
	}

	iter := db.ReadOnlyTransaction().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var tableName, tableType string
		if err := row.Columns(&tableName, &tableType); err != nil {
			return nil, err
		}
		attributes := []engine.Record{
			{Key: "Table Type", Value: tableType},
		}
		storageUnits = append(storageUnits, engine.StorageUnit{
			Name:       tableName,
			Attributes: attributes,
		})
	}

	return storageUnits, nil
}

func (p *SpannerPlugin) GetRows(config *engine.PluginConfig, schema string, storageUnit string, where string, pageSize int, pageOffset int) (*engine.GetRowsResult, error) {
	ctx := context.Background()
	db, err := DB(config)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var sqlString string

	if where == "" {
		sqlString = "SELECT * FROM " + storageUnit + " LIMIT @pageSize OFFSET @pageOffset"
	} else {
		sqlString = "SELECT * FROM " + storageUnit + " WHERE " + where + " LIMIT @pageSize OFFSET @pageOffset"
	}

	stmt := spannerdb.Statement{
		SQL: sqlString,
		Params: map[string]interface{}{
			"storageUnit": storageUnit,
			"where":       where,
			"pageSize":    pageSize,
			"pageOffset":  pageOffset,
		},
	}

	result := &engine.GetRowsResult{}
	iter := db.ReadOnlyTransaction().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		colums := make([]interface{}, row.Size())
		if err := row.Columns(colums...); err != nil {
			return nil, err
		}
		values := make([]string, len(colums))
		for i, c := range colums {
			values[i] = c.(string)
		}
		result.Rows = append(result.Rows, values)
	}

	return result, nil
}

func (p *SpannerPlugin) RawExecute(config *engine.PluginConfig, query string) (*engine.GetRowsResult, error) {
	return nil, errors.ErrUnsupported
}

func (p *SpannerPlugin) Chat(config *engine.PluginConfig, schema string, model string, previousConversation string, query string) ([]*engine.ChatMessage, error) {
	return nil, errors.ErrUnsupported
}

func NewSpannerPlugin() *engine.Plugin {
	return &engine.Plugin{
		Type:            engine.DatabaseType_Spanner,
		PluginFunctions: &SpannerPlugin{},
	}
}

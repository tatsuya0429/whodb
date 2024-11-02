package spanner

import (
	"context"
	"fmt"

	spannerdb "cloud.google.com/go/spanner"
	"github.com/clidey/whodb/core/src/engine"
	"google.golang.org/api/iterator"
)

type tableRelations struct {
	Table1   string
	Table2   string
	Relation string
}

const graphQuery = `
WITH fk_constraints AS (
    SELECT DISTINCT
        ccu.table_name AS table1,
        tc.table_name AS table2,
        'OneToMany' AS relation
    FROM 
        information_schema.table_constraints AS tc
    JOIN 
        information_schema.key_column_usage AS kcu
    ON 
        tc.constraint_name = kcu.constraint_name
    JOIN 
        information_schema.constraint_column_usage AS ccu
    ON 
        ccu.constraint_name = tc.constraint_name
    WHERE 
        tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = '%v'
        AND ccu.table_schema = '%v'
),
pk_constraints AS (
    SELECT DISTINCT
        tc.table_name AS table1,
        ccu.table_name AS table2,
        'OneToOne' AS relation
    FROM 
        information_schema.table_constraints AS tc
    JOIN 
        information_schema.key_column_usage AS kcu
    ON 
        tc.constraint_name = kcu.constraint_name
    JOIN 
        information_schema.constraint_column_usage AS ccu
    ON 
        ccu.constraint_name = tc.constraint_name
    WHERE 
        tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = '%v'
        AND ccu.table_schema = '%v'
        AND tc.table_name != ccu.table_name
),
unique_constraints AS (
    SELECT DISTINCT
        tc.table_name AS table1,
        ccu.table_name AS table2,
        'ManyToOne' AS relation
    FROM 
        information_schema.table_constraints AS tc
    JOIN 
        information_schema.key_column_usage AS kcu
    ON 
        tc.constraint_name = kcu.constraint_name
    JOIN 
        information_schema.constraint_column_usage AS ccu
    ON 
        ccu.constraint_name = tc.constraint_name
    WHERE 
        tc.constraint_type = 'UNIQUE'
        AND tc.table_schema = '%v'
        AND ccu.table_schema = '%v'
        AND tc.table_name != ccu.table_name
),
many_to_many_constraints AS (
    SELECT DISTINCT
        kcu1.table_name AS table1,
        kcu2.table_name AS table2,
        'ManyToMany' AS relation
    FROM
        information_schema.key_column_usage kcu1
    JOIN
        information_schema.referential_constraints rc
    ON
        kcu1.constraint_name = rc.constraint_name
    JOIN
        information_schema.key_column_usage kcu2
    ON
        kcu2.constraint_name = rc.unique_constraint_name
    WHERE
        kcu1.ordinal_position = 1 AND kcu2.ordinal_position = 2
        AND kcu1.table_schema = '%v'
        AND kcu2.table_schema = '%v'
)
SELECT * FROM fk_constraints
UNION
SELECT * FROM pk_constraints
UNION
SELECT * FROM unique_constraints
UNION
SELECT * FROM many_to_many_constraints
`

func (p *SpannerPlugin) GetGraph(config *engine.PluginConfig, schema string) ([]engine.GraphUnit, error) {
	ctx := context.Background()
	db, err := DB(config)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	tr := []tableRelations{}

	query := fmt.Sprintf(graphQuery, schema, schema, schema, schema, schema, schema, schema, schema)

	snapshot := db.ReadOnlyTransaction()
	defer snapshot.Close()

	rows := snapshot.Query(ctx, spannerdb.Statement{SQL: query})
	defer rows.Stop()

	for {
		var table1, table2, relation string
		row, err := rows.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		if err := row.Columns(&table1, &table2, &relation); err != nil {
			return nil, err
		}

		tr = append(tr, tableRelations{
			Table1:   table1,
			Table2:   table2,
			Relation: relation,
		})
	}

	tableMap := make(map[string][]engine.GraphUnitRelationship)
	for _, tri := range tr {
		tableMap[tri.Table1] = append(tableMap[tri.Table1], engine.GraphUnitRelationship{Name: tri.Table2, RelationshipType: engine.GraphUnitRelationshipType(tri.Relation)})
	}

	storageUnits, err := p.GetStorageUnits(config, schema)
	if err != nil {
		return nil, err
	}

	storageUnitsMap := map[string]engine.StorageUnit{}
	for _, storageUnit := range storageUnits {
		storageUnitsMap[storageUnit.Name] = storageUnit
	}

	graph := []engine.GraphUnit{}
	for _, storageUnit := range storageUnits {
		var relations []engine.GraphUnitRelationship
		foundTable, ok := tableMap[storageUnit.Name]
		if ok {
			relations = foundTable
		}
		graph = append(graph, engine.GraphUnit{
			Unit:      storageUnit,
			Relations: relations,
		})
	}

	return graph, nil
}

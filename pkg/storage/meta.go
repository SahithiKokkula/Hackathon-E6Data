package storage

import (
    "context"
    "database/sql"
)

func EnsureMetaTables(ctx context.Context, db *sql.DB) error {
    stmts := []string{
        `CREATE TABLE IF NOT EXISTS aqe_table_stats (
            table_name TEXT PRIMARY KEY,
            row_count INTEGER DEFAULT 0,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );`,
        `CREATE TABLE IF NOT EXISTS aqe_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            sample_table TEXT NOT NULL,
            sample_fraction REAL NOT NULL,
            strata_column TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );`,
        `CREATE TABLE IF NOT EXISTS aqe_sketches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            column_name TEXT,
            sketch_type TEXT NOT NULL,
            sketch_data BLOB NOT NULL,
            parameters TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(table_name, column_name, sketch_type)
        );`,
        `CREATE TABLE IF NOT EXISTS aqe_strata_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_table TEXT NOT NULL,
            strata_key TEXT NOT NULL,
            strata_value TEXT NOT NULL,
            pop_size INTEGER NOT NULL,
            sample_size INTEGER NOT NULL,
            fraction REAL NOT NULL,
            weight REAL NOT NULL,
            variance REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );`,
    }
    for _, s := range stmts {
        if _, err := db.ExecContext(ctx, s); err != nil { return err }
    }
    return nil
}

// UpsertTableRowCount sets the row_count for a table.
func UpsertTableRowCount(ctx context.Context, db *sql.DB, table string, count int64) error {
    _, err := db.ExecContext(ctx, `INSERT INTO aqe_table_stats(table_name,row_count,updated_at)
        VALUES(?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(table_name) DO UPDATE SET row_count=excluded.row_count, updated_at=CURRENT_TIMESTAMP`, table, count)
    return err
}

// InsertSampleMeta records a materialized sample.
func InsertSampleMeta(ctx context.Context, db *sql.DB, table, sampleTable string, fraction float64) error {
    _, err := db.ExecContext(ctx, `INSERT INTO aqe_samples(table_name,sample_table,sample_fraction,created_at)
        VALUES(?,?,?,CURRENT_TIMESTAMP)`, table, sampleTable, fraction)
    return err
}

// UpsertSketch stores or updates a sketch
func UpsertSketch(ctx context.Context, db *sql.DB, table, column, sketchType string, data []byte, parameters string) error {
    _, err := db.ExecContext(ctx, `
        INSERT INTO aqe_sketches(table_name, column_name, sketch_type, sketch_data, parameters, created_at)
        VALUES(?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(table_name, column_name, sketch_type) 
        DO UPDATE SET sketch_data=excluded.sketch_data, parameters=excluded.parameters, created_at=CURRENT_TIMESTAMP`,
        table, column, sketchType, data, parameters)
    return err
}

// GetSketch retrieves a sketch
func GetSketch(ctx context.Context, db *sql.DB, table, column, sketchType string) ([]byte, string, error) {
    var data []byte
    var parameters string
    err := db.QueryRowContext(ctx, `
        SELECT sketch_data, parameters FROM aqe_sketches 
        WHERE table_name = ? AND column_name = ? AND sketch_type = ?`,
        table, column, sketchType).Scan(&data, &parameters)
    return data, parameters, err
}

// ListSketches returns all sketches for a table
func ListSketches(ctx context.Context, db *sql.DB, table string) ([]SketchInfo, error) {
    rows, err := db.QueryContext(ctx, `
        SELECT column_name, sketch_type, parameters, 
               strftime('%s', created_at) as created_at
        FROM aqe_sketches 
        WHERE table_name = ?
        ORDER BY created_at DESC`, table)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var sketches []SketchInfo
    for rows.Next() {
        var info SketchInfo
        var column, sketchType, parameters string
        var createdAt int64
        
        err := rows.Scan(&column, &sketchType, &parameters, &createdAt)
        if err != nil {
            return nil, err
        }
        
        info.Table = table
        info.Column = column
        info.Type = SketchType(sketchType)
        info.CreatedAt = createdAt
        // Parse parameters if needed
        info.Parameters = make(map[string]interface{})
        
        sketches = append(sketches, info)
    }
    
    return sketches, rows.Err()
}

// SketchInfo contains metadata about a sketch
type SketchInfo struct {
    Type       SketchType `json:"type"`
    Table      string     `json:"table"`
    Column     string     `json:"column,omitempty"`
    CreatedAt  int64      `json:"created_at"`
    Parameters map[string]interface{} `json:"parameters"`
}

// SketchType represents the type of sketch
type SketchType string

const (
    HyperLogLogType   SketchType = "hyperloglog"
    CountMinSketchType SketchType = "countmin"
)

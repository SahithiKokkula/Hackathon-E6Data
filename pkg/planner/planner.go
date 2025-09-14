package planner

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// PlanType indicates which path to use
type PlanType string

const (
	PlanExact  PlanType = "exact"
	PlanSample PlanType = "sample"
	PlanSketch PlanType = "sketch"
)

type Plan struct {
	Type           PlanType `json:"type"`
	SQL            string   `json:"sql"`
	OriginalSQL    string   `json:"original_sql"`
	Table          string   `json:"table,omitempty"`
	SampleTable    string   `json:"sample_table,omitempty"`
	SampleFraction float64  `json:"sample_fraction,omitempty"`
	SketchType     string   `json:"sketch_type,omitempty"`
	SketchColumn   string   `json:"sketch_column,omitempty"`
	EstimatedCost  float64  `json:"estimated_cost"`
	EstimatedError float64  `json:"estimated_error"`
	Reason         string   `json:"reason"`
}

type QueryFeatures struct {
	HasDistinct    bool
	HasGroupBy     bool
	AggregateTypes []string
	GroupByColumns []string
	WhereColumns   []string
	IsHeavyHitter  bool
}

type CostModel struct {
	ScanCostPerRow   float64
	HashCostPerGroup float64
	SketchQueryCost  float64
	SampleSetupCost  float64
}

type Planner struct {
	costModel CostModel
}

func New() *Planner {
	return &Planner{
		costModel: CostModel{
			ScanCostPerRow:   1.0,
			HashCostPerGroup: 2.0,
			SketchQueryCost:  10.0,
			SampleSetupCost:  5.0,
		},
	}
}

var (
	fromRe     = regexp.MustCompile(`(?i)from\s+([a-zA-Z0-9_]+)`)
	distinctRe = regexp.MustCompile(`(?i)select\s+distinct|count\s*\(\s*distinct`)
	aggRe      = regexp.MustCompile(`(?i)(count|sum|avg|min|max)\s*\(`)
	groupByRe  = regexp.MustCompile(`(?i)group\s+by\s+([^having^order^limit]+)`)
	whereRe    = regexp.MustCompile(`(?i)where\s+([^group^order^limit]+)`)
)

func (p *Planner) Plan(ctx context.Context, db *sql.DB, sqlText string, maxRelError float64, preferExact bool) (*Plan, error) {
	features := p.parseQueryFeatures(sqlText)

	table := p.extractTableName(sqlText)
	if table == "" {
		return &Plan{Type: PlanExact, SQL: sqlText, OriginalSQL: sqlText, Reason: "no table found"}, nil
	}

	if originalTable, fraction, isSample := p.parseSampleTableName(table); isSample {
		return &Plan{
			Type:           PlanSample,
			SQL:            sqlText,
			OriginalSQL:    sqlText,
			Table:          originalTable,
			SampleTable:    table,
			SampleFraction: fraction,
			Reason:         fmt.Sprintf("direct query on sample table (fraction: %.4f)", fraction),
		}, nil
	}

	if preferExact {
		return &Plan{Type: PlanExact, SQL: sqlText, OriginalSQL: sqlText, Table: table, Reason: "user prefers exact"}, nil
	}

	tableStats, err := p.getTableStats(ctx, db, table)
	if err != nil {
		return &Plan{Type: PlanExact, SQL: sqlText, OriginalSQL: sqlText, Table: table, Reason: "no table stats available"}, nil
	}

	strategies := p.evaluateStrategies(ctx, db, sqlText, table, features, tableStats, maxRelError)

	bestStrategy := p.chooseBestStrategy(strategies, maxRelError)

	return bestStrategy, nil
}

func (p *Planner) parseQueryFeatures(sql string) QueryFeatures {
	features := QueryFeatures{}

	features.HasDistinct = distinctRe.MatchString(sql)

	aggMatches := aggRe.FindAllStringSubmatch(sql, -1)
	for _, match := range aggMatches {
		if len(match) > 1 {
			features.AggregateTypes = append(features.AggregateTypes, strings.ToUpper(match[1]))
		}
	}

	groupByMatch := groupByRe.FindStringSubmatch(sql)
	if len(groupByMatch) > 1 {
		features.HasGroupBy = true
		groupByCols := strings.Split(strings.TrimSpace(groupByMatch[1]), ",")
		for _, col := range groupByCols {
			col = strings.TrimSpace(col)
			if col != "" {
				features.GroupByColumns = append(features.GroupByColumns, col)
			}
		}
	}

	features.IsHeavyHitter = features.HasGroupBy && len(features.GroupByColumns) <= 2

	return features
}

func (p *Planner) extractTableName(sql string) string {
	match := fromRe.FindStringSubmatch(sql)
	if len(match) >= 2 {
		return match[1]
	}
	return ""
}

func (p *Planner) parseSampleTableName(tableName string) (string, float64, bool) {
	if strings.Contains(tableName, "__sample_") {
		if idx := strings.Index(tableName, "__sample_"); idx >= 0 {
			originalTable := tableName[:idx]
			fractionPart := tableName[idx+10:]

			fractionStr := strings.Replace(fractionPart, "_", ".", -1)
			if fraction, err := strconv.ParseFloat(fractionStr, 64); err == nil {
				return originalTable, fraction, true
			}
		}
	}

	if strings.Contains(tableName, "__strat_sample_") {
		if idx := strings.Index(tableName, "__strat_sample_"); idx >= 0 {
			originalTable := tableName[:idx]
			remaining := tableName[idx+15:]

			lastUnderscore := strings.LastIndex(remaining, "_")
			if lastUnderscore >= 0 {
				fractionPart := remaining[lastUnderscore+1:]
				fractionStr := strings.Replace(fractionPart, "_", ".", -1)
				if fraction, err := strconv.ParseFloat(fractionStr, 64); err == nil {
					return originalTable, fraction, true
				}
			}
		}
	}

	return tableName, 0, false
}

// TableStats contains table metadata for cost estimation
type TableStats struct {
	RowCount            int64
	DistinctValueCounts map[string]int64 // column -> distinct count
	HasSketches         map[string]bool  // column -> has sketch
	BestSampleFraction  float64
}

// getTableStats retrieves table statistics for planning
func (p *Planner) getTableStats(ctx context.Context, db *sql.DB, table string) (*TableStats, error) {
	stats := &TableStats{
		DistinctValueCounts: make(map[string]int64),
		HasSketches:         make(map[string]bool),
	}

	// Get row count
	err := db.QueryRowContext(ctx, "SELECT row_count FROM aqe_table_stats WHERE table_name = ?", table).Scan(&stats.RowCount)
	if err != nil {
		// Fallback: count directly
		err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&stats.RowCount)
		if err != nil {
			return nil, err
		}
	}

	// Check for available sketches
	rows, err := db.QueryContext(ctx, "SELECT column_name, sketch_type FROM aqe_sketches WHERE table_name = ?", table)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var column, sketchType string
			if err := rows.Scan(&column, &sketchType); err == nil {
				stats.HasSketches[column] = true
			}
		}
	}

	// Find best available sample
	var bestFraction float64
	err = db.QueryRowContext(ctx,
		"SELECT sample_fraction FROM aqe_samples WHERE table_name = ? ORDER BY sample_fraction ASC LIMIT 1",
		table).Scan(&bestFraction)
	if err == nil {
		stats.BestSampleFraction = bestFraction
	}

	return stats, nil
}

// evaluateStrategies generates and evaluates different execution plans
func (p *Planner) evaluateStrategies(ctx context.Context, db *sql.DB, sql, table string, features QueryFeatures, stats *TableStats, maxRelError float64) []*Plan {
	var strategies []*Plan

	// Strategy 1: Exact execution
	exactPlan := &Plan{
		Type:           PlanExact,
		SQL:            sql,
		OriginalSQL:    sql,
		Table:          table,
		EstimatedCost:  p.estimateExactCost(features, stats),
		EstimatedError: 0.0,
		Reason:         "exact execution",
	}
	strategies = append(strategies, exactPlan)

	// Strategy 2: Sketch-based (for DISTINCT or heavy-hitter queries)
	if features.HasDistinct {
		sketchPlan := p.evaluateSketchStrategy(sql, table, features, stats, "hyperloglog")
		if sketchPlan != nil {
			strategies = append(strategies, sketchPlan)
		}
	}

	if features.IsHeavyHitter {
		sketchPlan := p.evaluateSketchStrategy(sql, table, features, stats, "countmin")
		if sketchPlan != nil {
			strategies = append(strategies, sketchPlan)
		}
	}

	// Strategy 3: Sample-based
	if stats.BestSampleFraction > 0 {
		samplePlan := p.evaluateSampleStrategy(ctx, db, sql, table, features, stats)
		if samplePlan != nil {
			strategies = append(strategies, samplePlan)
		}
	}

	return strategies
}

// estimateExactCost estimates the cost of exact execution
func (p *Planner) estimateExactCost(features QueryFeatures, stats *TableStats) float64 {
	cost := float64(stats.RowCount) * p.costModel.ScanCostPerRow

	// Add GROUP BY cost
	if features.HasGroupBy {
		// Estimate number of groups (heuristic)
		estimatedGroups := math.Min(float64(stats.RowCount), 10000) // cap at 10k groups
		cost += estimatedGroups * p.costModel.HashCostPerGroup
	}

	return cost
}

// evaluateSketchStrategy creates a sketch-based plan if applicable
func (p *Planner) evaluateSketchStrategy(sql, table string, features QueryFeatures, stats *TableStats, sketchType string) *Plan {
	var column string
	var estimatedError float64

	if sketchType == "hyperloglog" && features.HasDistinct {
		// Extract DISTINCT column (simplified)
		if strings.Contains(strings.ToUpper(sql), "COUNT(DISTINCT") {
			// Try to extract column name
			column = "id" // simplified - would need better parsing
		}

		if stats.HasSketches[column] {
			// HyperLogLog standard error ≈ 1.04/√m, assume m=1024
			estimatedError = 1.04 / math.Sqrt(1024) // ≈ 3.25%

			return &Plan{
				Type:           PlanSketch,
				SQL:            sql, // Would need rewriting for sketch
				OriginalSQL:    sql,
				Table:          table,
				SketchType:     sketchType,
				SketchColumn:   column,
				EstimatedCost:  p.costModel.SketchQueryCost,
				EstimatedError: estimatedError,
				Reason:         "using HyperLogLog sketch for DISTINCT",
			}
		}
	}

	if sketchType == "countmin" && features.IsHeavyHitter {
		// Count-Min sketch for heavy hitters
		if len(features.GroupByColumns) > 0 {
			column = features.GroupByColumns[0]
		}

		if stats.HasSketches[column] {
			// Count-Min error ≈ ε * total_count, assume ε = 0.01
			estimatedError = 0.01 // 1%

			return &Plan{
				Type:           PlanSketch,
				SQL:            sql, // Would need rewriting for sketch
				OriginalSQL:    sql,
				Table:          table,
				SketchType:     sketchType,
				SketchColumn:   column,
				EstimatedCost:  p.costModel.SketchQueryCost,
				EstimatedError: estimatedError,
				Reason:         "using Count-Min sketch for heavy hitters",
			}
		}
	}

	return nil
}

// evaluateSampleStrategy creates a sample-based plan
func (p *Planner) evaluateSampleStrategy(ctx context.Context, db *sql.DB, sql, table string, features QueryFeatures, stats *TableStats) *Plan {
	sampleTable := fmt.Sprintf("%s__sample_%s", table, fractionName(stats.BestSampleFraction))

	// Check if sample table exists
	var exists int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
		sampleTable).Scan(&exists)

	if err != nil || exists == 0 {
		return nil // Sample doesn't exist
	}

	// Estimate sample error (simplified)
	estimatedError := math.Sqrt(1.0 / (stats.BestSampleFraction * float64(stats.RowCount)))

	// Rewrite SQL for sample (basic approach)
	rewrittenSQL := p.rewriteSQLForSample(sql, table, sampleTable, stats.BestSampleFraction)

	sampleCost := float64(stats.RowCount)*stats.BestSampleFraction*p.costModel.ScanCostPerRow + p.costModel.SampleSetupCost

	return &Plan{
		Type:           PlanSample,
		SQL:            rewrittenSQL,
		OriginalSQL:    sql,
		Table:          table,
		SampleTable:    sampleTable,
		SampleFraction: stats.BestSampleFraction,
		EstimatedCost:  sampleCost,
		EstimatedError: estimatedError,
		Reason:         fmt.Sprintf("using %.1f%% sample", stats.BestSampleFraction*100),
	}
}

// chooseBestStrategy selects the optimal execution plan
func (p *Planner) chooseBestStrategy(strategies []*Plan, maxRelError float64) *Plan {
	if len(strategies) == 0 {
		return &Plan{Type: PlanExact, Reason: "no strategies available"}
	}

	// Filter strategies that meet error requirement
	validStrategies := make([]*Plan, 0)
	for _, strategy := range strategies {
		if strategy.EstimatedError <= maxRelError {
			validStrategies = append(validStrategies, strategy)
		}
	}

	// If no strategy meets error requirement, use exact
	if len(validStrategies) == 0 {
		return strategies[0] // Assume first is exact
	}

	// Choose strategy with lowest cost among valid ones
	bestStrategy := validStrategies[0]
	for _, strategy := range validStrategies[1:] {
		if strategy.EstimatedCost < bestStrategy.EstimatedCost {
			bestStrategy = strategy
		}
	}

	return bestStrategy
}

// rewriteSQLForSample transforms SQL to use sample table
func (p *Planner) rewriteSQLForSample(sql, originalTable, sampleTable string, fraction float64) string {
	// Replace table name
	rewritten := strings.Replace(sql, originalTable, sampleTable, -1)

	// This is a simplified rewriting - production would need a proper SQL parser
	if strings.Contains(strings.ToUpper(rewritten), "COUNT(") {
		// Would need to wrap COUNT() with scaling
		// For now, leave as-is since scaling happens in executor
	}

	return rewritten
}

// Helper function to convert fraction to string
func fractionName(f float64) string {
	if f <= 0 {
		return "0_000"
	}
	s := fmt.Sprintf("%.3f", f)
	s = strings.Replace(s, ".", "_", 1)
	s = strings.TrimRight(s, "0")
	if strings.HasSuffix(s, "_") {
		s += "0"
	}
	return s
}

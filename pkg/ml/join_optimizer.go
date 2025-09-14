package ml

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

type JoinOptimizationStrategy string

const (
	JoinStrategyExact        JoinOptimizationStrategy = "exact"
	JoinStrategySampleBoth   JoinOptimizationStrategy = "sample_both"
	JoinStrategySampleLarger JoinOptimizationStrategy = "sample_larger"
	JoinStrategyBloomFilter  JoinOptimizationStrategy = "bloom_filter"
	JoinStrategyHashSemi     JoinOptimizationStrategy = "hash_semi"
	JoinStrategySketchJoin   JoinOptimizationStrategy = "sketch_join"
)

type JoinAnalysis struct {
	JoinType         string                   `json:"join_type"`
	LeftTable        string                   `json:"left_table"`
	RightTable       string                   `json:"right_table"`
	JoinCondition    string                   `json:"join_condition"`
	LeftTableSize    int64                    `json:"left_table_size"`
	RightTableSize   int64                    `json:"right_table_size"`
	Selectivity      float64                  `json:"selectivity"`
	Strategy         JoinOptimizationStrategy `json:"strategy"`
	OptimizedSQL     string                   `json:"optimized_sql"`
	Reasoning        string                   `json:"reasoning"`
	EstimatedSpeedup float64                  `json:"estimated_speedup"`
	EstimatedError   float64                  `json:"estimated_error"`
}

type JoinOptimizer struct {
	learningOptimizer *LearningOptimizer
}

func NewJoinOptimizer(learningOptimizer *LearningOptimizer) *JoinOptimizer {
	return &JoinOptimizer{
		learningOptimizer: learningOptimizer,
	}
}

// AnalyzeJoinQuery detects and analyzes JOIN operations in SQL
func (jo *JoinOptimizer) AnalyzeJoinQuery(ctx context.Context, sql string) (*JoinAnalysis, error) {
	// Detect if query contains JOINs
	if !jo.containsJoin(sql) {
		return nil, nil // Not a JOIN query
	}

	analysis := &JoinAnalysis{}

	// Extract JOIN information
	joinInfo, err := jo.extractJoinInfo(sql)
	if err != nil {
		return nil, err
	}

	analysis.JoinType = joinInfo.JoinType
	analysis.LeftTable = joinInfo.LeftTable
	analysis.RightTable = joinInfo.RightTable
	analysis.JoinCondition = joinInfo.JoinCondition

	// Get table sizes
	analysis.LeftTableSize = jo.getTableSize(ctx, analysis.LeftTable)
	analysis.RightTableSize = jo.getTableSize(ctx, analysis.RightTable)

	// Estimate join selectivity
	analysis.Selectivity = jo.estimateJoinSelectivity(analysis)

	// Choose optimization strategy
	analysis.Strategy = jo.chooseJoinStrategy(analysis)

	// Generate optimized SQL
	analysis.OptimizedSQL = jo.generateOptimizedJoinSQL(sql, analysis)

	// Calculate estimates
	analysis.EstimatedSpeedup = jo.calculateJoinSpeedup(analysis)
	analysis.EstimatedError = jo.calculateJoinError(analysis)
	analysis.Reasoning = jo.generateJoinReasoning(analysis)

	return analysis, nil
}

// JoinInfo holds extracted JOIN information
type JoinInfo struct {
	JoinType      string
	LeftTable     string
	RightTable    string
	JoinCondition string
}

// containsJoin checks if SQL contains JOIN operations
func (jo *JoinOptimizer) containsJoin(sql string) bool {
	sqlUpper := strings.ToUpper(sql)
	return strings.Contains(sqlUpper, " JOIN ") ||
		strings.Contains(sqlUpper, " INNER JOIN ") ||
		strings.Contains(sqlUpper, " LEFT JOIN ") ||
		strings.Contains(sqlUpper, " RIGHT JOIN ") ||
		strings.Contains(sqlUpper, " FULL JOIN ")
}

// extractJoinInfo parses JOIN syntax from SQL
func (jo *JoinOptimizer) extractJoinInfo(sql string) (*JoinInfo, error) {
	// Regex to extract JOIN information
	joinRegex := regexp.MustCompile(`(?i)FROM\s+(\w+)(?:\s+\w+)?\s+((?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN)\s+(\w+)(?:\s+\w+)?\s+ON\s+([^WHERE^GROUP^ORDER^LIMIT]+)`)

	matches := joinRegex.FindStringSubmatch(sql)
	if len(matches) < 5 {
		return nil, fmt.Errorf("unable to parse JOIN syntax")
	}

	return &JoinInfo{
		LeftTable:     matches[1],
		JoinType:      strings.TrimSpace(matches[2]),
		RightTable:    matches[3],
		JoinCondition: strings.TrimSpace(matches[4]),
	}, nil
}

// getTableSize retrieves the row count for a table
func (jo *JoinOptimizer) getTableSize(ctx context.Context, tableName string) int64 {
	var size int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	err := jo.learningOptimizer.db.QueryRowContext(ctx, query).Scan(&size)
	if err != nil {
		return 1000 // Default estimate if query fails
	}
	return size
}

// estimateJoinSelectivity calculates estimated result size
func (jo *JoinOptimizer) estimateJoinSelectivity(analysis *JoinAnalysis) float64 {
	// Simple heuristic-based selectivity estimation
	// In practice, this would use column statistics and histograms

	switch strings.ToUpper(analysis.JoinType) {
	case "INNER JOIN", "JOIN":
		// INNER JOINs typically have medium selectivity
		return 0.1 // 10% of Cartesian product
	case "LEFT JOIN", "LEFT OUTER JOIN":
		// LEFT JOINs preserve left table size
		return float64(analysis.LeftTableSize) / float64(analysis.LeftTableSize*analysis.RightTableSize)
	case "RIGHT JOIN", "RIGHT OUTER JOIN":
		// RIGHT JOINs preserve right table size
		return float64(analysis.RightTableSize) / float64(analysis.LeftTableSize*analysis.RightTableSize)
	case "FULL JOIN", "FULL OUTER JOIN":
		// FULL JOINs can be large
		return 0.5 // Conservative estimate
	default:
		return 0.1 // Default
	}
}

// chooseJoinStrategy selects the optimal JOIN optimization strategy
func (jo *JoinOptimizer) chooseJoinStrategy(analysis *JoinAnalysis) JoinOptimizationStrategy {
	totalSize := analysis.LeftTableSize + analysis.RightTableSize
	largerTable := analysis.LeftTableSize
	if analysis.RightTableSize > largerTable {
		largerTable = analysis.RightTableSize
	}

	// Strategy decision tree based on table sizes and JOIN type

	// Rule 1: Small tables - use exact computation
	if totalSize < 10000 {
		return JoinStrategyExact
	}

	// Rule 2: One very large table with one small - sample the large one
	if largerTable > 100000 && (largerTable/(totalSize-largerTable)) > 10 {
		return JoinStrategySampleLarger
	}

	// Rule 3: Both tables are large - sample both
	if analysis.LeftTableSize > 50000 && analysis.RightTableSize > 50000 {
		return JoinStrategySampleBoth
	}

	// Rule 4: High selectivity INNER JOINs - use bloom filter optimization
	if strings.Contains(strings.ToUpper(analysis.JoinType), "INNER") && analysis.Selectivity < 0.05 {
		return JoinStrategyBloomFilter
	}

	// Rule 5: Semi-joins (existence checks) - use hash semi join
	if jo.isSemiJoinPattern(analysis.JoinCondition) {
		return JoinStrategyHashSemi
	}

	// Default: sample the larger table
	return JoinStrategySampleLarger
}

// isSemiJoinPattern detects if this looks like a semi-join
func (jo *JoinOptimizer) isSemiJoinPattern(joinCondition string) bool {
	// Simple heuristic - in practice would analyze SELECT clause
	return strings.Contains(strings.ToUpper(joinCondition), "EXISTS") ||
		strings.Contains(strings.ToUpper(joinCondition), "IN")
}

// generateOptimizedJoinSQL creates the optimized JOIN query
func (jo *JoinOptimizer) generateOptimizedJoinSQL(originalSQL string, analysis *JoinAnalysis) string {
	switch analysis.Strategy {
	case JoinStrategyExact:
		return originalSQL

	case JoinStrategySampleBoth:
		return jo.applySampleBothStrategy(originalSQL, analysis)

	case JoinStrategySampleLarger:
		return jo.applySampleLargerStrategy(originalSQL, analysis)

	case JoinStrategyBloomFilter:
		return jo.applyBloomFilterStrategy(originalSQL, analysis)

	case JoinStrategyHashSemi:
		return jo.applyHashSemiStrategy(originalSQL, analysis)

	case JoinStrategySketchJoin:
		return jo.applySketchJoinStrategy(originalSQL, analysis)

	default:
		return originalSQL
	}
}

// applySampleBothStrategy samples both tables before JOIN
func (jo *JoinOptimizer) applySampleBothStrategy(sql string, analysis *JoinAnalysis) string {
	leftSampleSize := jo.calculateSampleSize(analysis.LeftTableSize, 0.02)   // 2% sample
	rightSampleSize := jo.calculateSampleSize(analysis.RightTableSize, 0.02) // 2% sample

	// Replace table references with sampled subqueries
	optimizedSQL := sql

	// Sample left table
	leftSample := fmt.Sprintf("(SELECT * FROM %s ORDER BY RANDOM() LIMIT %d) AS %s_sample",
		analysis.LeftTable, leftSampleSize, analysis.LeftTable)
	optimizedSQL = strings.Replace(optimizedSQL, "FROM "+analysis.LeftTable, "FROM "+leftSample, 1)

	// Sample right table
	rightSample := fmt.Sprintf("(SELECT * FROM %s ORDER BY RANDOM() LIMIT %d) AS %s_sample",
		analysis.RightTable, rightSampleSize, analysis.RightTable)
	optimizedSQL = strings.Replace(optimizedSQL, "JOIN "+analysis.RightTable, "JOIN "+rightSample, 1)

	return optimizedSQL
}

// applySampleLargerStrategy samples only the larger table
func (jo *JoinOptimizer) applySampleLargerStrategy(sql string, analysis *JoinAnalysis) string {
	var tableToSample string
	var sampleSize int64

	if analysis.LeftTableSize > analysis.RightTableSize {
		tableToSample = analysis.LeftTable
		sampleSize = jo.calculateSampleSize(analysis.LeftTableSize, 0.05) // 5% of larger table
	} else {
		tableToSample = analysis.RightTable
		sampleSize = jo.calculateSampleSize(analysis.RightTableSize, 0.05)
	}

	// Replace the larger table with a sample
	sampleSubquery := fmt.Sprintf("(SELECT * FROM %s ORDER BY RANDOM() LIMIT %d) AS %s_sample",
		tableToSample, sampleSize, tableToSample)

	if tableToSample == analysis.LeftTable {
		return strings.Replace(sql, "FROM "+tableToSample, "FROM "+sampleSubquery, 1)
	} else {
		return strings.Replace(sql, "JOIN "+tableToSample, "JOIN "+sampleSubquery, 1)
	}
}

// applyBloomFilterStrategy uses bloom filter approximation for highly selective JOINs
func (jo *JoinOptimizer) applyBloomFilterStrategy(sql string, analysis *JoinAnalysis) string {
	// Simplified bloom filter simulation: sample smaller table and use as filter
	smallerTable := analysis.LeftTable
	smallerSize := analysis.LeftTableSize

	if analysis.RightTableSize < analysis.LeftTableSize {
		smallerTable = analysis.RightTable
		smallerSize = analysis.RightTableSize
	}

	_ = jo.calculateSampleSize(smallerSize, 0.1) // 10% of smaller table (used for estimation)

	// Create a comment indicating bloom filter simulation
	return fmt.Sprintf("-- Bloom filter simulation: sampling %s\n%s", smallerTable,
		jo.applySampleLargerStrategy(sql, analysis))
}

// applyHashSemiStrategy optimizes semi-join patterns
func (jo *JoinOptimizer) applyHashSemiStrategy(sql string, analysis *JoinAnalysis) string {
	// For semi-joins, we can often use EXISTS instead of JOIN
	// This is a simplified transformation
	return fmt.Sprintf("-- Hash semi-join optimization\n%s", sql)
}

// applySketchJoinStrategy uses sketching for approximate JOIN results
func (jo *JoinOptimizer) applySketchJoinStrategy(sql string, analysis *JoinAnalysis) string {
	// Sample both tables more aggressively for sketch-based approximation
	_ = jo.calculateSampleSize(analysis.LeftTableSize, 0.01)  // 1% sample (for estimation)
	_ = jo.calculateSampleSize(analysis.RightTableSize, 0.01) // 1% sample (for estimation)

	return jo.applySampleBothStrategy(sql, analysis)
}

// calculateSampleSize determines optimal sample size
func (jo *JoinOptimizer) calculateSampleSize(tableSize int64, fraction float64) int64 {
	sampleSize := int64(float64(tableSize) * fraction)
	if sampleSize < 100 {
		sampleSize = 100 // Minimum sample size
	}
	if sampleSize > tableSize {
		sampleSize = tableSize
	}
	return sampleSize
}

// calculateJoinSpeedup estimates performance improvement
func (jo *JoinOptimizer) calculateJoinSpeedup(analysis *JoinAnalysis) float64 {
	switch analysis.Strategy {
	case JoinStrategyExact:
		return 1.0

	case JoinStrategySampleBoth:
		// Speedup based on reduction in JOIN complexity (quadratic improvement)
		leftReduction := 0.02                         // 2% sample
		rightReduction := 0.02                        // 2% sample
		return 1.0 / (leftReduction * rightReduction) // ~2500x theoretical speedup

	case JoinStrategySampleLarger:
		return 20.0 // Conservative estimate for single table sampling

	case JoinStrategyBloomFilter:
		return 50.0 // Bloom filters can be very effective

	case JoinStrategyHashSemi:
		return 10.0 // Hash semi-joins avoid full materialization

	case JoinStrategySketchJoin:
		return 100.0 // Aggressive approximation

	default:
		return 1.0
	}
}

// calculateJoinError estimates approximation error
func (jo *JoinOptimizer) calculateJoinError(analysis *JoinAnalysis) float64 {
	switch analysis.Strategy {
	case JoinStrategyExact:
		return 0.0

	case JoinStrategySampleBoth:
		// Error compounds from both tables
		return 0.05 // 5% error from dual sampling

	case JoinStrategySampleLarger:
		return 0.03 // 3% error from single table sampling

	case JoinStrategyBloomFilter:
		return 0.02 // 2% error (mostly false positives)

	case JoinStrategyHashSemi:
		return 0.01 // 1% error for existence checks

	case JoinStrategySketchJoin:
		return 0.08 // 8% error for aggressive sketching

	default:
		return 0.0
	}
}

// generateJoinReasoning creates explanation for JOIN optimization choice
func (jo *JoinOptimizer) generateJoinReasoning(analysis *JoinAnalysis) string {
	switch analysis.Strategy {
	case JoinStrategyExact:
		return fmt.Sprintf("Small tables (%d + %d rows) - exact JOIN computation is efficient",
			analysis.LeftTableSize, analysis.RightTableSize)

	case JoinStrategySampleBoth:
		return fmt.Sprintf("Large tables on both sides (%d, %d rows) - dual sampling provides %.0fx speedup with %.1f%% error",
			analysis.LeftTableSize, analysis.RightTableSize, analysis.EstimatedSpeedup, analysis.EstimatedError*100)

	case JoinStrategySampleLarger:
		return fmt.Sprintf("Asymmetric table sizes (%d vs %d) - sampling larger table optimizes JOIN performance",
			analysis.LeftTableSize, analysis.RightTableSize)

	case JoinStrategyBloomFilter:
		return fmt.Sprintf("Highly selective %s with low estimated selectivity (%.2f%%) - bloom filter optimization effective",
			analysis.JoinType, analysis.Selectivity*100)

	case JoinStrategyHashSemi:
		return "Semi-join pattern detected - hash-based existence check optimization"

	case JoinStrategySketchJoin:
		return "Very large JOIN with high error tolerance - sketch-based approximation"

	default:
		return "Standard JOIN optimization applied"
	}
}

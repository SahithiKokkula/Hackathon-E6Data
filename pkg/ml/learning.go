package ml

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"regexp"
	"time"
)

type QueryPerformanceHistory struct {
	ID               int64     `json:"id"`
	QueryPattern     string    `json:"query_pattern"`
	TableSize        int64     `json:"table_size"`
	Strategy         string    `json:"strategy"`
	ActualSpeedup    float64   `json:"actual_speedup"`
	ActualError      float64   `json:"actual_error"`
	PredictedSpeedup float64   `json:"predicted_speedup"`
	PredictedError   float64   `json:"predicted_error"`
	ExecutionTimeMs  int64     `json:"execution_time_ms"`
	ErrorTolerance   float64   `json:"error_tolerance"`
	UserSatisfaction int       `json:"user_satisfaction"`
	Timestamp        time.Time `json:"timestamp"`
	QueryFeatures    string    `json:"query_features"`
}

type LearningOptimizer struct {
	*MLOptimizer
	learningEnabled bool
}

func NewLearningOptimizer(db *sql.DB) *LearningOptimizer {
	return &LearningOptimizer{
		MLOptimizer:     NewMLOptimizer(db),
		learningEnabled: true,
	}
}

func (lo *LearningOptimizer) OptimizeQueryWithLearning(ctx context.Context, originalSQL string, errorTolerance float64) (*QueryOptimization, error) {
	features, err := lo.extractQueryFeatures(ctx, originalSQL, errorTolerance)
	if err != nil {
		return lo.OptimizeQuery(ctx, originalSQL, errorTolerance)
	}

	joinOptimizer := NewJoinOptimizer(lo)
	joinAnalysis, err := joinOptimizer.AnalyzeJoinQuery(ctx, originalSQL)
	if err == nil && joinAnalysis != nil {
		return &QueryOptimization{
			Strategy:         OptimizationStrategy(joinAnalysis.Strategy),
			ModifiedSQL:      joinAnalysis.OptimizedSQL,
			OriginalSQL:      originalSQL,
			Confidence:       0.85,
			EstimatedSpeedup: joinAnalysis.EstimatedSpeedup,
			EstimatedError:   joinAnalysis.EstimatedError,
			Reasoning:        joinAnalysis.Reasoning,
			Transformations:  []string{fmt.Sprintf("Applied %s JOIN optimization", joinAnalysis.Strategy)},
			JoinAnalysis:     joinAnalysis,
		}, nil
	}

	if err := lo.ensurePerformanceHistoryTable(ctx); err != nil {
		log.Printf("Warning: Could not create performance history table: %v", err)
		return lo.OptimizeQuery(ctx, originalSQL, errorTolerance)
	}

	historicalPerf, err := lo.getHistoricalPerformance(ctx, features)
	if err != nil {
		log.Printf("Warning: Could not fetch historical performance: %v", err)
	}

	strategy, confidence := lo.chooseStrategyWithLearning(features, historicalPerf)

	modifiedSQL, transformations, speedup, estimatedError := lo.applyTransformationsWithLearning(ctx, originalSQL, strategy, features, historicalPerf)

	optimization := &QueryOptimization{
		Strategy:         strategy,
		ModifiedSQL:      modifiedSQL,
		OriginalSQL:      originalSQL,
		Confidence:       confidence,
		EstimatedSpeedup: speedup,
		EstimatedError:   estimatedError,
		Reasoning:        lo.generateLearningReasoning(strategy, features, historicalPerf),
		Transformations:  transformations,
	}

	return optimization, nil
}

// RecordQueryPerformance stores actual execution results for learning
func (lo *LearningOptimizer) RecordQueryPerformance(ctx context.Context,
	optimization *QueryOptimization,
	features *QueryFeatures,
	actualExecutionTime time.Duration,
	actualError float64,
	baselineExecutionTime time.Duration) error {

	if !lo.learningEnabled {
		return nil
	}

	actualSpeedup := float64(baselineExecutionTime) / float64(actualExecutionTime)
	if actualSpeedup < 0.1 {
		actualSpeedup = 0.1 // Prevent division issues
	}

	queryPattern := lo.normalizeQueryPattern(optimization.OriginalSQL)
	featuresJSON, _ := json.Marshal(features)

	perf := &QueryPerformanceHistory{
		QueryPattern:     queryPattern,
		TableSize:        features.TableSize,
		Strategy:         string(optimization.Strategy),
		ActualSpeedup:    actualSpeedup,
		ActualError:      actualError,
		PredictedSpeedup: optimization.EstimatedSpeedup,
		PredictedError:   optimization.EstimatedError,
		ExecutionTimeMs:  actualExecutionTime.Milliseconds(),
		ErrorTolerance:   features.ErrorTolerance,
		UserSatisfaction: 0, // Can be set later via feedback API
		Timestamp:        time.Now(),
		QueryFeatures:    string(featuresJSON),
	}

	return lo.storePerformanceHistory(ctx, perf)
}

// ensurePerformanceHistoryTable creates the learning table if it doesn't exist
func (lo *LearningOptimizer) ensurePerformanceHistoryTable(ctx context.Context) error {
	createSQL := `
	CREATE TABLE IF NOT EXISTS ml_query_performance_history (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		query_pattern TEXT NOT NULL,
		table_size INTEGER NOT NULL,
		strategy TEXT NOT NULL,
		actual_speedup REAL NOT NULL,
		actual_error REAL NOT NULL,
		predicted_speedup REAL NOT NULL,
		predicted_error REAL NOT NULL,
		execution_time_ms INTEGER NOT NULL,
		error_tolerance REAL NOT NULL,
		user_satisfaction INTEGER DEFAULT 0,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		query_features TEXT
	)`

	if _, err := lo.db.ExecContext(ctx, createSQL); err != nil {
		return err
	}

	// Create indexes separately
	indexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_query_pattern ON ml_query_performance_history(query_pattern)`,
		`CREATE INDEX IF NOT EXISTS idx_table_size ON ml_query_performance_history(table_size)`,
		`CREATE INDEX IF NOT EXISTS idx_strategy ON ml_query_performance_history(strategy)`,
		`CREATE INDEX IF NOT EXISTS idx_timestamp ON ml_query_performance_history(timestamp)`,
	}

	for _, indexSQL := range indexes {
		if _, err := lo.db.ExecContext(ctx, indexSQL); err != nil {
			log.Printf("Warning: Could not create index: %v", err)
			// Continue even if index creation fails
		}
	}

	return nil
}

// getHistoricalPerformance retrieves similar query performance data
func (lo *LearningOptimizer) getHistoricalPerformance(ctx context.Context, features *QueryFeatures) ([]*QueryPerformanceHistory, error) {
	// Find similar queries based on pattern and table size
	query := `
	SELECT id, query_pattern, table_size, strategy, actual_speedup, actual_error,
		   predicted_speedup, predicted_error, execution_time_ms, error_tolerance,
		   user_satisfaction, timestamp, query_features
	FROM ml_query_performance_history 
	WHERE table_size BETWEEN ? AND ?
	AND error_tolerance BETWEEN ? AND ?
	ORDER BY timestamp DESC 
	LIMIT 50`

	tableSizeRange := float64(features.TableSize) * 0.5 // ±50% table size
	errorRange := features.ErrorTolerance * 0.5         // ±50% error tolerance

	rows, err := lo.db.QueryContext(ctx, query,
		int64(float64(features.TableSize)-tableSizeRange),
		int64(float64(features.TableSize)+tableSizeRange),
		features.ErrorTolerance-errorRange,
		features.ErrorTolerance+errorRange,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []*QueryPerformanceHistory
	for rows.Next() {
		var h QueryPerformanceHistory
		err := rows.Scan(&h.ID, &h.QueryPattern, &h.TableSize, &h.Strategy,
			&h.ActualSpeedup, &h.ActualError, &h.PredictedSpeedup, &h.PredictedError,
			&h.ExecutionTimeMs, &h.ErrorTolerance, &h.UserSatisfaction,
			&h.Timestamp, &h.QueryFeatures)
		if err != nil {
			continue
		}
		history = append(history, &h)
	}

	return history, nil
}

// chooseStrategyWithLearning uses historical data to improve strategy selection
func (lo *LearningOptimizer) chooseStrategyWithLearning(features *QueryFeatures, history []*QueryPerformanceHistory) (OptimizationStrategy, float64) {
	// If no historical data, use base strategy
	if len(history) == 0 {
		return lo.chooseStrategy(features)
	}

	// Analyze historical performance by strategy
	strategyPerformance := make(map[OptimizationStrategy]*StrategyStats)

	for _, h := range history {
		strategy := OptimizationStrategy(h.Strategy)
		if strategyPerformance[strategy] == nil {
			strategyPerformance[strategy] = &StrategyStats{}
		}

		stats := strategyPerformance[strategy]
		stats.Count++
		stats.TotalSpeedupAccuracy += math.Abs(h.ActualSpeedup-h.PredictedSpeedup) / h.PredictedSpeedup
		stats.TotalErrorAccuracy += math.Abs(h.ActualError-h.PredictedError) / math.Max(h.PredictedError, 0.01)
		stats.AvgSpeedup += h.ActualSpeedup
		stats.AvgError += h.ActualError
		stats.AvgSatisfaction += float64(h.UserSatisfaction)
	}

	// Calculate average performance for each strategy
	bestStrategy := StrategyExact
	bestScore := 0.0

	for strategy, stats := range strategyPerformance {
		if stats.Count == 0 {
			continue
		}

		avgSpeedup := stats.AvgSpeedup / float64(stats.Count)
		avgError := stats.AvgError / float64(stats.Count)
		speedupAccuracy := 1.0 - (stats.TotalSpeedupAccuracy / float64(stats.Count))
		errorAccuracy := 1.0 - (stats.TotalErrorAccuracy / float64(stats.Count))

		// Composite score: balance speedup, error, and prediction accuracy
		score := avgSpeedup*0.4 +
			(1.0-avgError)*0.3 +
			speedupAccuracy*0.2 +
			errorAccuracy*0.1

		if score > bestScore && avgError <= features.ErrorTolerance*1.2 { // Allow 20% tolerance buffer
			bestScore = score
			bestStrategy = strategy
		}
	}

	// Calculate confidence based on historical accuracy
	confidence := 0.6 // Base confidence
	if stats, exists := strategyPerformance[bestStrategy]; exists && stats.Count > 0 {
		speedupAccuracy := 1.0 - (stats.TotalSpeedupAccuracy / float64(stats.Count))
		errorAccuracy := 1.0 - (stats.TotalErrorAccuracy / float64(stats.Count))
		confidence = 0.3 + 0.7*(speedupAccuracy+errorAccuracy)/2.0
	}

	return bestStrategy, math.Min(confidence, 0.95)
}

// StrategyStats holds performance statistics for a strategy
type StrategyStats struct {
	Count                int
	TotalSpeedupAccuracy float64
	TotalErrorAccuracy   float64
	AvgSpeedup           float64
	AvgError             float64
	AvgSatisfaction      float64
}

// applyTransformationsWithLearning uses learned parameters for transformations
func (lo *LearningOptimizer) applyTransformationsWithLearning(ctx context.Context, originalSQL string, strategy OptimizationStrategy, features *QueryFeatures, history []*QueryPerformanceHistory) (string, []string, float64, float64) {
	// Use base transformations but adjust parameters based on learning
	modifiedSQL, transformations, speedup, estimatedError := lo.applyTransformations(ctx, originalSQL, strategy, features)

	// Adjust estimates based on historical accuracy
	if len(history) > 0 {
		var speedupAdjustment, errorAdjustment float64
		count := 0

		for _, h := range history {
			if OptimizationStrategy(h.Strategy) == strategy {
				speedupAdjustment += h.ActualSpeedup / h.PredictedSpeedup
				errorAdjustment += h.ActualError / h.PredictedError
				count++
			}
		}

		if count > 0 {
			speedupAdjustment /= float64(count)
			errorAdjustment /= float64(count)

			// Apply learned adjustments (with dampening to prevent overcorrection)
			speedup *= (1.0 + (speedupAdjustment-1.0)*0.3)
			estimatedError *= (1.0 + (errorAdjustment-1.0)*0.3)

			transformations = append(transformations, fmt.Sprintf("Applied learning adjustments (speedup: %.2fx, error: %.2fx)", speedupAdjustment, errorAdjustment))
		}
	}

	return modifiedSQL, transformations, speedup, estimatedError
}

// storePerformanceHistory saves execution results for learning
func (lo *LearningOptimizer) storePerformanceHistory(ctx context.Context, perf *QueryPerformanceHistory) error {
	insertSQL := `
	INSERT INTO ml_query_performance_history 
	(query_pattern, table_size, strategy, actual_speedup, actual_error, 
	 predicted_speedup, predicted_error, execution_time_ms, error_tolerance, 
	 user_satisfaction, timestamp, query_features)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := lo.db.ExecContext(ctx, insertSQL,
		perf.QueryPattern, perf.TableSize, perf.Strategy, perf.ActualSpeedup,
		perf.ActualError, perf.PredictedSpeedup, perf.PredictedError,
		perf.ExecutionTimeMs, perf.ErrorTolerance, perf.UserSatisfaction,
		perf.Timestamp, perf.QueryFeatures)

	return err
}

// normalizeQueryPattern creates a pattern from SQL for similarity matching
func (lo *LearningOptimizer) normalizeQueryPattern(sql string) string {
	// Simple normalization - replace specific values with placeholders
	// This could be made more sophisticated with proper SQL parsing
	pattern := sql

	// Normalize common patterns
	pattern = regexp.MustCompile(`\b\d+\b`).ReplaceAllString(pattern, "?")
	pattern = regexp.MustCompile(`'[^']*'`).ReplaceAllString(pattern, "?")
	pattern = regexp.MustCompile(`"[^"]*"`).ReplaceAllString(pattern, "?")

	return pattern
}

// generateLearningReasoning creates explanations that include learning insights
func (lo *LearningOptimizer) generateLearningReasoning(strategy OptimizationStrategy, features *QueryFeatures, history []*QueryPerformanceHistory) string {
	baseReasoning := lo.generateReasoning(strategy, features)

	if len(history) == 0 {
		return baseReasoning + " (No historical data available)"
	}

	// Add learning insights
	strategyCount := 0
	avgSpeedup := 0.0
	avgError := 0.0

	for _, h := range history {
		if OptimizationStrategy(h.Strategy) == strategy {
			strategyCount++
			avgSpeedup += h.ActualSpeedup
			avgError += h.ActualError
		}
	}

	if strategyCount > 0 {
		avgSpeedup /= float64(strategyCount)
		avgError /= float64(strategyCount)

		return fmt.Sprintf("%s (Learned from %d similar queries: avg %.1fx speedup, %.1f%% error)",
			baseReasoning, strategyCount, avgSpeedup, avgError*100)
	}

	return baseReasoning + fmt.Sprintf(" (Analyzed %d historical queries)", len(history))
}

// GetLearningStats returns statistics about the learning system
func (lo *LearningOptimizer) GetLearningStats(ctx context.Context) (map[string]interface{}, error) {
	query := `
	SELECT 
		strategy,
		COUNT(*) as query_count,
		AVG(actual_speedup) as avg_speedup,
		AVG(actual_error) as avg_error,
		AVG(ABS(actual_speedup - predicted_speedup) / predicted_speedup) as speedup_prediction_error,
		AVG(ABS(actual_error - predicted_error) / CASE WHEN predicted_error > 0 THEN predicted_error ELSE 0.01 END) as error_prediction_error
	FROM ml_query_performance_history 
	WHERE timestamp > datetime('now', '-30 days')
	GROUP BY strategy`

	rows, err := lo.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := make(map[string]interface{})
	strategies := make(map[string]map[string]float64)

	for rows.Next() {
		var strategy string
		var queryCount int
		var avgSpeedup, avgError, speedupPredError, errorPredError float64

		err := rows.Scan(&strategy, &queryCount, &avgSpeedup, &avgError, &speedupPredError, &errorPredError)
		if err != nil {
			continue
		}

		strategies[strategy] = map[string]float64{
			"query_count":                 float64(queryCount),
			"avg_speedup":                 avgSpeedup,
			"avg_error":                   avgError,
			"speedup_prediction_accuracy": 1.0 - speedupPredError,
			"error_prediction_accuracy":   1.0 - errorPredError,
		}
	}

	stats["strategies"] = strategies
	stats["learning_enabled"] = lo.learningEnabled

	// Get total historical data count
	var totalQueries int
	lo.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ml_query_performance_history").Scan(&totalQueries)
	stats["total_historical_queries"] = totalQueries

	return stats, nil
}

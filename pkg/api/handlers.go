package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sahithikokkula/Hackathon-E6Data/aqe/pkg/executor"
	"github.com/sahithikokkula/Hackathon-E6Data/aqe/pkg/ml"
	"github.com/sahithikokkula/Hackathon-E6Data/aqe/pkg/planner"
	"github.com/sahithikokkula/Hackathon-E6Data/aqe/pkg/sampler"
	"github.com/sahithikokkula/Hackathon-E6Data/aqe/pkg/sketches"
	"github.com/sahithikokkula/Hackathon-E6Data/aqe/pkg/storage"
)

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, JSON{"status": "ok"})
}

func (h *Handler) ListTables(w http.ResponseWriter, r *http.Request) {
	rows, err := h.db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY 1`)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, JSON{"error": err.Error()})
		return
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		tables = append(tables, name)
	}
	writeJSON(w, http.StatusOK, JSON{"tables": tables})
}

type QueryRequest struct {
	SQL               string  `json:"sql"`
	MaxRelError       float64 `json:"max_rel_error"`
	PreferExact       bool    `json:"prefer_exact"`
	UseMLOptimization bool    `json:"use_ml_optimization"`
	Explain           bool    `json:"explain"`
}

type QueryResponse struct {
	Status            string                `json:"status"`
	Plan              *planner.Plan         `json:"plan,omitempty"`
	Result            []map[string]any      `json:"result,omitempty"`
	Meta              map[string]any        `json:"meta,omitempty"`
	Error             string                `json:"error,omitempty"`
	MLOptimization    *ml.QueryOptimization `json:"ml_optimization,omitempty"`
	StatisticalBounds *ml.StatisticalBounds `json:"statistical_bounds,omitempty"`
}

func (h *Handler) PostQuery(w http.ResponseWriter, r *http.Request) {
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, JSON{"error": "invalid json"})
		return
	}
	req.SQL = strings.TrimSpace(req.SQL)
	if req.SQL == "" {
		writeJSON(w, http.StatusBadRequest, JSON{"error": "sql required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()

	var mlOptimization *ml.QueryOptimization
	var statisticalBounds *ml.StatisticalBounds
	var finalSQL = req.SQL
	var learningOptimizer *ml.LearningOptimizer

	if req.UseMLOptimization && !req.PreferExact {
		learningOptimizer = ml.NewLearningOptimizer(h.db)
		var err error
		mlOptimization, err = learningOptimizer.OptimizeQueryWithLearning(ctx, req.SQL, req.MaxRelError)
		if err != nil {
			mlOptimization = &ml.QueryOptimization{
				Strategy:        ml.StrategyExact,
				ModifiedSQL:     req.SQL,
				OriginalSQL:     req.SQL,
				Reasoning:       fmt.Sprintf("ML optimization failed: %v", err),
				Transformations: make([]string, 0),
			}
		} else {
			finalSQL = mlOptimization.ModifiedSQL
		}
	}

	p := planner.New()
	plan, err := p.Plan(ctx, h.db, finalSQL, req.MaxRelError, req.PreferExact)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, JSON{"error": err.Error()})
		return
	}

	if req.Explain {
		writeJSON(w, http.StatusOK, QueryResponse{
			Status:         "ok",
			Plan:           plan,
			MLOptimization: mlOptimization,
		})
		return
	}

	executionStart := time.Now()

	rows, meta, err := executor.Execute(ctx, h.db, plan)
	executionTime := time.Since(executionStart)

	if err != nil {
		writeJSON(w, http.StatusInternalServerError, QueryResponse{
			Status:         "error",
			Error:          err.Error(),
			Plan:           plan,
			MLOptimization: mlOptimization,
		})
		return
	}

	if req.UseMLOptimization && mlOptimization != nil && mlOptimization.Strategy == ml.StrategySample {
		scaleMLOptimizedResults(rows, mlOptimization)

		errorEstimator := ml.NewErrorEstimator(0.95)

		sampleSize := int64(2000)
		populationSize := int64(200000)
		samplingFraction := float64(sampleSize) / float64(populationSize)

		aggregationCols := identifyAggregationColumns(rows)

		for _, col := range aggregationCols {
			if len(rows) > 0 {
				if val, exists := rows[0][col]; exists {
					if numVal, ok := convertToFloat64API(val); ok {
						bounds := errorEstimator.EstimateErrorBounds(
							numVal, sampleSize, populationSize, samplingFraction,
							getAggregationType(col))

						errorEstimator.ApplyStatisticalBoundsToResults(rows, bounds, []string{col})

						if statisticalBounds == nil {
							statisticalBounds = bounds
						}
					}
				}
			}
		}
	}

	// Record ML learning performance for ALL optimization strategies, not just sampling
	// BUT skip recording if we're querying the ML learning table itself to prevent recursion
	sqlLower := strings.ToLower(req.SQL)
	isMLHistoryQuery := strings.Contains(sqlLower, "ml_query_performance_history")
	if req.UseMLOptimization && mlOptimization != nil && !isMLHistoryQuery {
		go func() {
			// Add panic recovery to prevent server crashes
			defer func() {
				if r := recover(); r != nil {
					// Log the panic but don't crash the server
					fmt.Printf("Panic in ML learning goroutine: %v\n", r)
				}
			}()

			// Use existing learning optimizer or create one if needed
			currentLearningOptimizer := learningOptimizer
			if currentLearningOptimizer == nil {
				currentLearningOptimizer = ml.NewLearningOptimizer(h.db)
			}

			// Add timeout context to prevent hanging
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Extract proper features using the optimizer instance
			features, err := currentLearningOptimizer.ExtractQueryFeatures(ctx, req.SQL, req.MaxRelError)
			if err != nil {
				// Fallback to basic features if extraction fails
				features = &ml.QueryFeatures{
					TableSize:      200000, // Default fallback
					ErrorTolerance: req.MaxRelError,
					QueryLength:    len(req.SQL),
					HasCount:       strings.Contains(strings.ToUpper(req.SQL), "COUNT"),
					HasSum:         strings.Contains(strings.ToUpper(req.SQL), "SUM"),
					HasGroupBy:     strings.Contains(strings.ToUpper(req.SQL), "GROUP BY"),
				}
			}
			// Validate ML optimization data before recording
			if mlOptimization.EstimatedSpeedup <= 0 {
				mlOptimization.EstimatedSpeedup = 1.0
			}
			if mlOptimization.EstimatedError < 0 {
				mlOptimization.EstimatedError = 0.0
			}

			actualError := 0.02
			baselineTime := executionTime * time.Duration(mlOptimization.EstimatedSpeedup)

			// Add error handling for RecordQueryPerformance
			err = currentLearningOptimizer.RecordQueryPerformance(
				ctx, mlOptimization, features,
				executionTime, actualError, baselineTime)
			if err != nil {
				fmt.Printf("Error recording ML performance: %v\n", err)
			}
		}()
	}

	// For ML history queries, clean up the response to prevent JSON serialization issues
	if isMLHistoryQuery && mlOptimization != nil {
		// Reset any potentially problematic fields in mlOptimization
		if math.IsInf(mlOptimization.EstimatedSpeedup, 0) || math.IsNaN(mlOptimization.EstimatedSpeedup) {
			mlOptimization.EstimatedSpeedup = 1.0
		}
		if math.IsInf(mlOptimization.EstimatedError, 0) || math.IsNaN(mlOptimization.EstimatedError) {
			mlOptimization.EstimatedError = 0.0
		}
		if math.IsInf(mlOptimization.Confidence, 0) || math.IsNaN(mlOptimization.Confidence) {
			mlOptimization.Confidence = 0.95
		}
	}

	// Validate ML optimization data before writing response
	if mlOptimization != nil {
		// Fix any NaN or Inf values that would break JSON serialization
		if math.IsNaN(mlOptimization.EstimatedError) || math.IsInf(mlOptimization.EstimatedError, 0) {
			mlOptimization.EstimatedError = 0.01
		}
		if math.IsNaN(mlOptimization.EstimatedSpeedup) || math.IsInf(mlOptimization.EstimatedSpeedup, 0) {
			mlOptimization.EstimatedSpeedup = 1.0
		}
		if math.IsNaN(mlOptimization.Confidence) || math.IsInf(mlOptimization.Confidence, 0) {
			mlOptimization.Confidence = 0.95
		}

		// Also check transformations for invalid content
		validTransformations := make([]string, 0, len(mlOptimization.Transformations))
		for _, t := range mlOptimization.Transformations {
			if !strings.Contains(t, "NaN") && !strings.Contains(t, "+Inf") && !strings.Contains(t, "-Inf") {
				validTransformations = append(validTransformations, t)
			} else {
				validTransformations = append(validTransformations, "Applied learning adjustments")
			}
		}
		mlOptimization.Transformations = validTransformations
	}

	log.Printf("About to write response with ML optimization: %+v", mlOptimization)

	writeJSON(w, http.StatusOK, QueryResponse{
		Status:            "ok",
		Plan:              plan,
		Result:            rows,
		Meta:              meta,
		MLOptimization:    mlOptimization,
		StatisticalBounds: statisticalBounds,
	})
}

type CreateSampleRequest struct {
	Table          string  `json:"table"`
	SampleFraction float64 `json:"sample_fraction"`
}

func (h *Handler) PostCreateSample(w http.ResponseWriter, r *http.Request) {
	var req CreateSampleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, JSON{"error": "invalid json"})
		return
	}
	if req.Table == "" || req.SampleFraction <= 0 || req.SampleFraction >= 1 {
		writeJSON(w, http.StatusBadRequest, JSON{"error": "table and 0<sample_fraction<1 required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	name, count, err := sampler.CreateUniformSample(ctx, h.db, req.Table, req.SampleFraction)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, JSON{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, JSON{"status": "ok", "sample_table": name, "rows": count})
}

func (h *Handler) GetLearningStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	learningOptimizer := ml.NewLearningOptimizer(h.db)
	stats, err := learningOptimizer.GetLearningStats(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, JSON{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, JSON{"status": "ok", "learning_stats": stats})
}

func (h *Handler) PostCreateStratifiedSample(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Table          string  `json:"table"`
		StrataColumn   string  `json:"strata_column"`
		TotalFraction  float64 `json:"total_fraction"`
		VarianceColumn string  `json:"variance_column,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, JSON{"error": "invalid json"})
		return
	}

	if req.Table == "" || req.StrataColumn == "" || req.TotalFraction <= 0 || req.TotalFraction >= 1 {
		writeJSON(w, http.StatusBadRequest, JSON{"error": "table, strata_column and 0<total_fraction<1 required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	sampleName, strata, err := sampler.CreateStratifiedSample(ctx, h.db, req.Table, req.StrataColumn, req.TotalFraction, req.VarianceColumn)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, JSON{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, JSON{
		"status":       "ok",
		"sample_table": sampleName,
		"strata":       strata,
		"allocation_type": func() string {
			if req.VarianceColumn != "" {
				return "neyman"
			}
			return "proportional"
		}(),
	})
}

func (h *Handler) PostCreateSketch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Table      string                 `json:"table"`
		Column     string                 `json:"column,omitempty"`
		SketchType string                 `json:"sketch_type"`
		Parameters map[string]interface{} `json:"parameters,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, JSON{"error": "invalid json"})
		return
	}

	if req.Table == "" || req.SketchType == "" {
		writeJSON(w, http.StatusBadRequest, JSON{"error": "table and sketch_type required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	var sketchData []byte
	var err error

	switch req.SketchType {
	case "hyperloglog":
		sketchData, err = h.createHyperLogLogSketch(ctx, req.Table, req.Column)
	case "countmin":
		sketchData, err = h.createCountMinSketch(ctx, req.Table, req.Column, req.Parameters)
	default:
		writeJSON(w, http.StatusBadRequest, JSON{"error": "unsupported sketch type"})
		return
	}

	if err != nil {
		writeJSON(w, http.StatusInternalServerError, JSON{"error": err.Error()})
		return
	}

	parametersJSON, _ := json.Marshal(req.Parameters)
	err = storage.UpsertSketch(ctx, h.db, req.Table, req.Column, req.SketchType, sketchData, string(parametersJSON))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, JSON{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, JSON{"status": "ok", "sketch_type": req.SketchType, "size_bytes": len(sketchData)})
}

func (h *Handler) GetSketches(w http.ResponseWriter, r *http.Request) {
	table := r.URL.Query().Get("table")
	if table == "" {
		writeJSON(w, http.StatusBadRequest, JSON{"error": "table parameter required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	sketches, err := storage.ListSketches(ctx, h.db, table)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, JSON{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, JSON{"sketches": sketches})
}

func (h *Handler) createHyperLogLogSketch(ctx context.Context, table, column string) ([]byte, error) {
	if column == "" {
		return nil, fmt.Errorf("column required for HyperLogLog")
	}

	hll := sketches.NewHyperLogLog(12)

	query := fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL", column, table, column)
	rows, err := h.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		hll.AddString(value)
		count++

		if count > 1000000 {
			break
		}
	}

	return hll.Serialize(), nil
}

func (h *Handler) createCountMinSketch(ctx context.Context, table, column string, parameters map[string]interface{}) ([]byte, error) {
	epsilon := 0.01
	delta := 0.01

	if eps, ok := parameters["epsilon"].(float64); ok {
		epsilon = eps
	}
	if d, ok := parameters["delta"].(float64); ok {
		delta = d
	}

	cms := sketches.NewCountMinSketch(epsilon, delta)

	var query string
	if column != "" {
		query = fmt.Sprintf("SELECT %s, COUNT(*) FROM %s WHERE %s IS NOT NULL GROUP BY %s", column, table, column, column)
	} else {
		query = fmt.Sprintf("SELECT 'total', COUNT(*) FROM %s", table)
	}

	rows, err := h.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		var count uint64
		if err := rows.Scan(&key, &count); err != nil {
			return nil, err
		}
		cms.AddString(key, count)
	}

	return cms.Serialize(), nil
}

func scaleMLOptimizedResults(results []map[string]any, mlOpt *ml.QueryOptimization) {
	if mlOpt == nil || mlOpt.Strategy != ml.StrategySample || len(results) == 0 {
		return
	}

	sampleFraction := 0.01
	for _, transform := range mlOpt.Transformations {
		if strings.Contains(transform, "fraction:") {
			if parts := strings.Split(transform, "fraction: "); len(parts) > 1 {
				if parsed := strings.TrimSuffix(strings.Split(parts[1], ")")[0], ")"); parsed != "" {
					if val, err := strconv.ParseFloat(parsed, 64); err == nil {
						sampleFraction = val
						break
					}
				}
			}
		}
	}

	if sampleFraction <= 0 {
		return
	}

	scale := 1.0 / sampleFraction

	for i := range results {
		for col, val := range results[i] {
			colUpper := strings.ToUpper(col)
			needsScaling := strings.Contains(colUpper, "COUNT") ||
				strings.Contains(colUpper, "SUM") ||
				strings.Contains(colUpper, "TOTAL") ||
				strings.Contains(colUpper, "REVENUE") ||
				strings.Contains(colUpper, "ORDERS")

			if needsScaling {
				if numVal, ok := convertToFloat64API(val); ok {
					results[i][col] = numVal * scale
				}
			}
		}
	}
}

func convertToFloat64API(val any) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case int:
		return float64(v), true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func identifyAggregationColumns(results []map[string]any) []string {
	if len(results) == 0 {
		return nil
	}

	var aggCols []string
	for col := range results[0] {
		colUpper := strings.ToUpper(col)
		if strings.Contains(colUpper, "COUNT") ||
			strings.Contains(colUpper, "SUM") ||
			strings.Contains(colUpper, "AVG") ||
			strings.Contains(colUpper, "TOTAL") ||
			strings.Contains(colUpper, "REVENUE") ||
			strings.Contains(colUpper, "ORDERS") {
			aggCols = append(aggCols, col)
		}
	}
	return aggCols
}

func getAggregationType(columnName string) string {
	colUpper := strings.ToUpper(columnName)
	if strings.Contains(colUpper, "COUNT") {
		return "COUNT"
	}
	if strings.Contains(colUpper, "SUM") || strings.Contains(colUpper, "TOTAL") || strings.Contains(colUpper, "REVENUE") {
		return "SUM"
	}
	if strings.Contains(colUpper, "AVG") || strings.Contains(colUpper, "MEAN") {
		return "AVG"
	}
	if strings.Contains(colUpper, "DISTINCT") {
		return "DISTINCT"
	}
	return "COUNT"
}

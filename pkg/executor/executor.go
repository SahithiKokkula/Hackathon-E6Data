package executor

import (
	"context"
	"database/sql"
	"strconv"
	"strings"

	"github.com/sahithikokkula/Hackathon-E6Data/aqe/pkg/estimator"
	"github.com/sahithikokkula/Hackathon-E6Data/aqe/pkg/planner"
)

func Execute(ctx context.Context, db *sql.DB, plan *planner.Plan) ([]map[string]any, map[string]any, error) {
	rows, err := db.QueryContext(ctx, plan.SQL)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	res := make([]map[string]any, 0, 64)
	var sampleData map[string][]float64

	if plan.Type == planner.PlanSample {
		sampleData = make(map[string][]float64)
		for _, col := range cols {
			sampleData[col] = make([]float64, 0)
		}
	}

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}

		m := map[string]any{}
		for i, c := range cols {
			m[c] = vals[i]

			if plan.Type == planner.PlanSample {
				if val, ok := convertToFloat64(vals[i]); ok {
					sampleData[c] = append(sampleData[c], val)
				}
			}
		}
		res = append(res, m)
	}

	meta := map[string]any{
		"plan_type":    string(plan.Type),
		"reason":       plan.Reason,
		"rows":         len(res),
		"sql_executed": plan.SQL,
	}

	if plan.Type == planner.PlanSample {
		meta["sample_fraction"] = plan.SampleFraction
		meta["sample_table"] = plan.SampleTable

		if len(res) > 0 {
			scaleSampleResults(res, plan.SampleFraction, cols)
			enrichWithBootstrapCIs(res, sampleData, plan.SampleFraction, cols)
		}
	}

	return res, meta, nil
}

func convertToFloat64(val any) (float64, bool) {
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

func scaleSampleResults(results []map[string]any, sampleFraction float64, cols []string) {
	if sampleFraction <= 0 || len(results) == 0 || len(cols) == 0 {
		return
	}

	scale := 1.0 / sampleFraction

	for i := range results {
		for _, col := range cols {
			val, exists := results[i][col]
			if !exists {
				continue
			}

			colUpper := strings.ToUpper(col)
			needsScaling := strings.Contains(colUpper, "COUNT") ||
				strings.Contains(colUpper, "SUM") ||
				strings.Contains(colUpper, "TOTAL") ||
				strings.Contains(colUpper, "REVENUE")

			if needsScaling {
				if numVal, ok := convertToFloat64(val); ok {
					results[i][col] = numVal * scale
				}
			}
		}
	}
}

func enrichWithBootstrapCIs(results []map[string]any, sampleData map[string][]float64, sampleFraction float64, cols []string) {
	const B = 300
	scale := 1.0 / sampleFraction

	for _, col := range cols {
		values, exists := sampleData[col]
		if !exists || len(values) == 0 {
			continue
		}

		var scaleFunc func([]float64) float64
		if strings.Contains(strings.ToUpper(col), "SUM") || strings.Contains(strings.ToUpper(col), "REVENUE") || strings.Contains(strings.ToUpper(col), "TOTAL") {
			scaleFunc = func(vals []float64) float64 {
				sum := 0.0
				for _, v := range vals {
					sum += v
				}
				return sum
			}
		} else {
			scaleFunc = func(vals []float64) float64 {
				if len(vals) == 0 {
					return 0
				}
				sum := 0.0
				for _, v := range vals {
					sum += v
				}
				return sum / float64(len(vals))
			}
		}

		ci := estimator.BootstrapCI(values, scaleFunc, scale, B, 0.95)

		for i := range results {
			if _, exists := results[i][col]; exists {
				results[i][col+"_ci_low"] = ci.Lower
				results[i][col+"_ci_high"] = ci.Upper
				results[i][col+"_rel_error"] = ci.RelativeError
			}
		}
	}
}

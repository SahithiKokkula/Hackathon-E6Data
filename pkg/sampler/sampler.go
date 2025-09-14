package sampler

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
)

func CreateUniformSample(ctx context.Context, db *sql.DB, table string, fraction float64) (string, int64, error) {
	if fraction <= 0 || fraction >= 1 {
		return "", 0, fmt.Errorf("invalid fraction")
	}
	name := fmt.Sprintf("%s__sample_%s", table, fractionName(fraction))
	_, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	if err != nil {
		return "", 0, err
	}
	q := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s WHERE (abs(random())/9223372036854775807.0) < %f", name, table, fraction)
	if _, err := db.ExecContext(ctx, q); err != nil {
		return "", 0, err
	}
	var cnt int64
	row := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", name))
	if err := row.Scan(&cnt); err != nil {
		return name, 0, err
	}
	_ = recordSampleMeta(ctx, db, table, name, fraction)
	return name, cnt, nil
}

func fractionName(f float64) string {
	if f <= 0 {
		return "0_000"
	}
	prec := 3
	if f < 0.001 {
		prec = 6
	}
	s := fmt.Sprintf("%.*f", prec, f)
	s = strings.Replace(s, ".", "_", 1)
	s = strings.TrimRight(s, "0")
	if strings.HasSuffix(s, "_") {
		s += "0"
	}
	if len(s) > 12 {
		e := int(math.Log10(f))
		mant := f / math.Pow(10, float64(e))
		s = fmt.Sprintf("%0.2fE%d", mant, e)
		s = strings.ReplaceAll(s, ".", "_")
		s = strings.ReplaceAll(s, "+", "p")
		s = strings.ReplaceAll(s, "-", "m")
	}
	if !strings.HasPrefix(s, "0_") {
		s = "0_" + s
	}
	return s
}

func recordSampleMeta(ctx context.Context, db *sql.DB, table, sample string, fraction float64) error {
	var baseCnt int64
	_ = db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", table)).Scan(&baseCnt)
	_, _ = db.ExecContext(ctx, `INSERT INTO aqe_table_stats(table_name,row_count,updated_at)
        VALUES(?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(table_name) DO UPDATE SET row_count=excluded.row_count, updated_at=CURRENT_TIMESTAMP`, table, baseCnt)
	_, _ = db.ExecContext(ctx, `INSERT INTO aqe_samples(table_name,sample_table,sample_fraction,created_at)
        VALUES(?,?,?,CURRENT_TIMESTAMP)`, table, sample, fraction)
	return nil
}

type StrataInfo struct {
	StrataKey   string  `json:"strata_key"`
	StrataValue string  `json:"strata_value"`
	PopSize     int64   `json:"pop_size"`
	SampleSize  int64   `json:"sample_size"`
	Fraction    float64 `json:"fraction"`
	Weight      float64 `json:"weight"`
	Variance    float64 `json:"variance"`
}

func CreateStratifiedSample(ctx context.Context, db *sql.DB, table string, strataCol string, totalFraction float64, varianceCol string) (string, []StrataInfo, error) {
	if totalFraction <= 0 || totalFraction >= 1 {
		return "", nil, fmt.Errorf("invalid total fraction: %f", totalFraction)
	}

	strata, err := analyzeStrata(ctx, db, table, strataCol, varianceCol)
	if err != nil {
		return "", nil, fmt.Errorf("failed to analyze strata: %w", err)
	}

	if varianceCol != "" {
		allocateNeymanOptimal(strata, totalFraction)
	} else {
		allocateProportional(strata, totalFraction)
	}
	sampleName := fmt.Sprintf("%s__strat_sample_%s_%s", table, strataCol, fractionName(totalFraction))

	// Drop existing sample if it exists
	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", sampleName))
	if err != nil {
		return "", nil, err
	}

	// Build the stratified sampling query
	query := buildStratifiedSampleQuery(table, sampleName, strataCol, strata)

	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create stratified sample: %w", err)
	}

	// Update actual sample sizes
	err = updateActualSampleSizes(ctx, db, sampleName, strataCol, strata)
	if err != nil {
		return "", nil, fmt.Errorf("failed to update sample sizes: %w", err)
	}

	// Record metadata
	err = recordStratifiedSampleMeta(ctx, db, table, sampleName, strataCol, totalFraction, strata)
	if err != nil {
		return "", nil, fmt.Errorf("failed to record metadata: %w", err)
	}

	return sampleName, strata, nil
}

// analyzeStrata discovers strata and their characteristics
func analyzeStrata(ctx context.Context, db *sql.DB, table, strataCol, varianceCol string) ([]StrataInfo, error) {
	var query string
	if varianceCol != "" {
		// Include variance calculation for Neyman allocation
		query = fmt.Sprintf(`
            SELECT %s as strata_value, 
                   COUNT(*) as pop_size,
                   AVG(%s) as mean_val,
                   CASE WHEN COUNT(*) > 1 THEN 
                       (SUM((%s - (SELECT AVG(%s) FROM %s WHERE %s = t.%s)) * (%s - (SELECT AVG(%s) FROM %s WHERE %s = t.%s))) / (COUNT(*) - 1))
                   ELSE 0 END as variance
            FROM %s t
            WHERE %s IS NOT NULL AND %s IS NOT NULL
            GROUP BY %s
            ORDER BY pop_size DESC`,
			strataCol, varianceCol, varianceCol, varianceCol, table, strataCol, strataCol,
			varianceCol, varianceCol, table, strataCol, strataCol,
			table, strataCol, varianceCol, strataCol)
	} else {
		query = fmt.Sprintf(`
            SELECT %s as strata_value, 
                   COUNT(*) as pop_size,
                   0.0 as mean_val,
                   0.0 as variance
            FROM %s
            WHERE %s IS NOT NULL
            GROUP BY %s
            ORDER BY pop_size DESC`,
			strataCol, table, strataCol, strataCol)
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var strata []StrataInfo
	for rows.Next() {
		var info StrataInfo
		var meanVal float64
		err := rows.Scan(&info.StrataValue, &info.PopSize, &meanVal, &info.Variance)
		if err != nil {
			return nil, err
		}
		info.StrataKey = strataCol
		strata = append(strata, info)
	}

	return strata, rows.Err()
}

// allocateNeymanOptimal uses Neyman allocation for optimal variance reduction
func allocateNeymanOptimal(strata []StrataInfo, totalFraction float64) {
	// Calculate total population and total "weight" (N_h * σ_h)
	var totalPop int64
	var totalWeight float64

	for i := range strata {
		totalPop += strata[i].PopSize
		stdDev := math.Sqrt(strata[i].Variance)
		strata[i].Weight = float64(strata[i].PopSize) * stdDev
		totalWeight += strata[i].Weight
	}

	totalSampleSize := float64(totalPop) * totalFraction

	// Allocate samples proportional to N_h * σ_h
	for i := range strata {
		if totalWeight > 0 {
			strata[i].SampleSize = int64(totalSampleSize * strata[i].Weight / totalWeight)
			strata[i].Fraction = float64(strata[i].SampleSize) / float64(strata[i].PopSize)
		} else {
			// Fallback to proportional allocation if no variance info
			strata[i].Fraction = totalFraction
			strata[i].SampleSize = int64(float64(strata[i].PopSize) * totalFraction)
		}

		// Ensure we don't sample more than the population
		if strata[i].Fraction > 1.0 {
			strata[i].Fraction = 1.0
			strata[i].SampleSize = strata[i].PopSize
		}
	}
}

// allocateProportional uses proportional allocation when no variance info available
func allocateProportional(strata []StrataInfo, totalFraction float64) {
	for i := range strata {
		strata[i].Fraction = totalFraction
		strata[i].SampleSize = int64(float64(strata[i].PopSize) * totalFraction)
		strata[i].Weight = float64(strata[i].PopSize)
	}
}

// buildStratifiedSampleQuery constructs the SQL for creating stratified sample
func buildStratifiedSampleQuery(table, sampleName, strataCol string, strata []StrataInfo) string {
	var unionParts []string

	for _, stratum := range strata {
		if stratum.SampleSize > 0 {
			// For each stratum, sample with the calculated fraction
			part := fmt.Sprintf(`
                SELECT * FROM %s 
                WHERE %s = '%s' AND (abs(random())/9223372036854775807.0) < %f`,
				table, strataCol, stratum.StrataValue, stratum.Fraction)
			unionParts = append(unionParts, part)
		}
	}

	if len(unionParts) == 0 {
		return fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s WHERE 1=0", sampleName, table)
	}

	query := fmt.Sprintf("CREATE TABLE %s AS %s", sampleName, strings.Join(unionParts, " UNION ALL "))
	return query
}

// updateActualSampleSizes updates the StrataInfo with actual achieved sample sizes
func updateActualSampleSizes(ctx context.Context, db *sql.DB, sampleName, strataCol string, strata []StrataInfo) error {
	query := fmt.Sprintf(`
        SELECT %s as strata_value, COUNT(*) as actual_count
        FROM %s
        GROUP BY %s`,
		strataCol, sampleName, strataCol)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	actualCounts := make(map[string]int64)
	for rows.Next() {
		var strataValue string
		var count int64
		if err := rows.Scan(&strataValue, &count); err != nil {
			return err
		}
		actualCounts[strataValue] = count
	}

	// Update the strata info with actual counts
	for i := range strata {
		if actualCount, exists := actualCounts[strata[i].StrataValue]; exists {
			strata[i].SampleSize = actualCount
			strata[i].Fraction = float64(actualCount) / float64(strata[i].PopSize)
		}
	}

	return rows.Err()
}

// recordStratifiedSampleMeta records metadata about the stratified sample
func recordStratifiedSampleMeta(ctx context.Context, db *sql.DB, table, sampleName, strataCol string, totalFraction float64, strata []StrataInfo) error {
	// Record in main samples table
	_, err := db.ExecContext(ctx, `
        INSERT INTO aqe_samples(table_name, sample_table, sample_fraction, strata_column, created_at)
        VALUES(?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		table, sampleName, totalFraction, strataCol)

	if err != nil {
		return err
	}

	// Create strata info table if it doesn't exist
	_, err = db.ExecContext(ctx, `
        CREATE TABLE IF NOT EXISTS aqe_strata_info (
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
        )`)

	if err != nil {
		return err
	}

	// Record each stratum's info
	for _, stratum := range strata {
		_, err = db.ExecContext(ctx, `
            INSERT INTO aqe_strata_info(sample_table, strata_key, strata_value, pop_size, sample_size, fraction, weight, variance)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
			sampleName, stratum.StrataKey, stratum.StrataValue, stratum.PopSize,
			stratum.SampleSize, stratum.Fraction, stratum.Weight, stratum.Variance)

		if err != nil {
			return err
		}
	}

	return nil
}

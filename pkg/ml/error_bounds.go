package ml

import (
	"fmt"
	"math"
)

type ConfidenceInterval struct {
	Lower      float64 `json:"lower"`
	Upper      float64 `json:"upper"`
	Confidence float64 `json:"confidence"`
	Method     string  `json:"method"`
}

type StatisticalBounds struct {
	RelativeError      float64             `json:"relative_error"`
	AbsoluteError      float64             `json:"absolute_error"`
	ConfidenceInterval *ConfidenceInterval `json:"confidence_interval"`
	SampleSize         int64               `json:"sample_size"`
	PopulationSize     int64               `json:"population_size"`
	SamplingMethod     string              `json:"sampling_method"`
	BiasCorrection     float64             `json:"bias_correction"`
	VarianceEstimate   float64             `json:"variance_estimate"`
}

type ErrorEstimator struct {
	confidenceLevel float64
}

func NewErrorEstimator(confidenceLevel float64) *ErrorEstimator {
	if confidenceLevel <= 0 || confidenceLevel >= 1 {
		confidenceLevel = 0.95
	}
	return &ErrorEstimator{
		confidenceLevel: confidenceLevel,
	}
}

func (ee *ErrorEstimator) EstimateErrorBounds(
	sampleValue float64,
	sampleSize int64,
	populationSize int64,
	samplingFraction float64,
	aggregationType string) *StatisticalBounds {

	// Calculate basic relative error using Central Limit Theorem
	relativeError := ee.calculateSamplingError(sampleSize, samplingFraction)

	// Calculate absolute error
	absoluteError := sampleValue * relativeError

	// Estimate variance based on aggregation type and sample characteristics
	variance := ee.estimateVariance(sampleValue, sampleSize, aggregationType)

	// Apply finite population correction if applicable
	if populationSize > 0 && samplingFraction > 0.05 {
		fpc := math.Sqrt((float64(populationSize) - float64(sampleSize)) / (float64(populationSize) - 1))
		variance *= fpc * fpc
		relativeError *= fpc
	}

	// Calculate confidence interval
	confidenceInterval := ee.calculateConfidenceInterval(sampleValue, variance, sampleSize)

	// Estimate bias correction for different aggregation types
	biasCorrection := ee.estimateBiasCorrection(aggregationType, sampleSize, samplingFraction)

	return &StatisticalBounds{
		RelativeError:      relativeError,
		AbsoluteError:      absoluteError,
		ConfidenceInterval: confidenceInterval,
		SampleSize:         sampleSize,
		PopulationSize:     populationSize,
		SamplingMethod:     "uniform_random",
		BiasCorrection:     biasCorrection,
		VarianceEstimate:   variance,
	}
}

// calculateSamplingError estimates relative error using statistical theory
func (ee *ErrorEstimator) calculateSamplingError(sampleSize int64, samplingFraction float64) float64 {
	if sampleSize <= 1 {
		return 0.5 // 50% error for invalid samples
	}

	// For large samples, use Central Limit Theorem approximation
	// Standard error of the mean = σ/√n, where σ ≈ √p(1-p) for proportions
	// For aggregations, we use a more conservative estimate

	effectiveSampleSize := float64(sampleSize)
	if effectiveSampleSize < 30 {
		effectiveSampleSize = 30 // Minimum for normal approximation
	}

	// Base error from sample size
	baseError := 1.0 / math.Sqrt(effectiveSampleSize)

	// Adjust for sampling fraction (smaller fractions = higher uncertainty)
	if samplingFraction > 0 && samplingFraction < 1.0 {
		fractionAdjustment := 1.0 + (1.0-samplingFraction)*0.5
		baseError *= fractionAdjustment
	}

	// Cap error bounds
	if baseError > 0.50 {
		return 0.50 // Maximum 50% relative error
	}
	if baseError < 0.005 {
		return 0.005 // Minimum 0.5% relative error
	}

	return baseError
}

// estimateVariance calculates variance estimate based on aggregation type
func (ee *ErrorEstimator) estimateVariance(sampleValue float64, sampleSize int64, aggregationType string) float64 {
	if sampleSize <= 1 {
		return sampleValue * sampleValue // High variance for small samples
	}

	baseVariance := sampleValue * sampleValue / float64(sampleSize)

	// Adjust variance based on aggregation type
	switch aggregationType {
	case "COUNT":
		// Poisson-like variance for counts
		return math.Max(sampleValue, 1.0) / float64(sampleSize)

	case "SUM":
		// Higher variance for sums due to accumulation
		return baseVariance * 2.0

	case "AVG", "MEAN":
		// Lower variance for averages (more stable)
		return baseVariance * 0.5

	case "DISTINCT":
		// High variance for distinct counts (depends on cardinality)
		return baseVariance * 3.0

	default:
		return baseVariance
	}
}

// calculateConfidenceInterval computes statistical confidence bounds
func (ee *ErrorEstimator) calculateConfidenceInterval(estimate float64, variance float64, sampleSize int64) *ConfidenceInterval {
	if sampleSize <= 1 || variance <= 0 {
		// Return wide interval for unreliable estimates
		return &ConfidenceInterval{
			Lower:      estimate * 0.5,
			Upper:      estimate * 1.5,
			Confidence: ee.confidenceLevel,
			Method:     "default_wide",
		}
	}

	standardError := math.Sqrt(variance)

	// Choose appropriate distribution based on sample size
	var criticalValue float64
	var method string

	if sampleSize >= 30 {
		// Use normal distribution for large samples
		criticalValue = ee.getNormalCriticalValue(ee.confidenceLevel)
		method = "normal"
	} else {
		// Use t-distribution for small samples
		criticalValue = ee.getTCriticalValue(ee.confidenceLevel, sampleSize-1)
		method = "t-distribution"
	}

	marginOfError := criticalValue * standardError

	return &ConfidenceInterval{
		Lower:      math.Max(0, estimate-marginOfError), // Ensure non-negative
		Upper:      estimate + marginOfError,
		Confidence: ee.confidenceLevel,
		Method:     method,
	}
}

// getNormalCriticalValue returns z-score for normal distribution
func (ee *ErrorEstimator) getNormalCriticalValue(confidence float64) float64 {
	// Common z-scores for confidence levels
	switch confidence {
	case 0.90:
		return 1.645
	case 0.95:
		return 1.960
	case 0.99:
		return 2.576
	default:
		// Approximate for other confidence levels
		alpha := 1.0 - confidence
		return math.Sqrt(2.0) * math.Erfinv(1.0-alpha)
	}
}

// getTCriticalValue returns t-score for t-distribution (simplified approximation)
func (ee *ErrorEstimator) getTCriticalValue(confidence float64, degreesOfFreedom int64) float64 {
	// Simplified t-table lookup - in practice, you'd use a more complete implementation
	normalValue := ee.getNormalCriticalValue(confidence)

	if degreesOfFreedom >= 30 {
		return normalValue
	}

	// Simple adjustment for small samples (conservative estimate)
	adjustment := 1.0 + (30.0-float64(degreesOfFreedom))/100.0
	return normalValue * adjustment
}

// estimateBiasCorrection calculates bias adjustment for different aggregation types
func (ee *ErrorEstimator) estimateBiasCorrection(aggregationType string, sampleSize int64, samplingFraction float64) float64 {
	if sampleSize <= 1 {
		return 0.0
	}

	switch aggregationType {
	case "COUNT":
		// COUNT estimates are unbiased with uniform sampling
		return 0.0

	case "SUM":
		// SUM estimates are unbiased with uniform sampling
		return 0.0

	case "AVG", "MEAN":
		// Average estimates are unbiased
		return 0.0

	case "DISTINCT":
		// DISTINCT counts have negative bias with small samples
		if samplingFraction < 0.1 {
			return -0.05 * (1.0 - samplingFraction) // Up to 5% negative bias
		}
		return 0.0

	case "VARIANCE", "STDDEV":
		// Variance estimates have small negative bias
		return -1.0 / float64(sampleSize)

	default:
		return 0.0
	}
}

// ApplyStatisticalBoundsToResults adds confidence intervals to query results
func (ee *ErrorEstimator) ApplyStatisticalBoundsToResults(
	results []map[string]any,
	bounds *StatisticalBounds,
	aggregationColumns []string) {

	if len(results) == 0 || bounds == nil {
		return
	}

	for i := range results {
		for _, col := range aggregationColumns {
			if val, exists := results[i][col]; exists {
				if numVal, ok := convertToFloat64(val); ok {
					// Apply confidence intervals
					if bounds.ConfidenceInterval != nil {
						// Scale the confidence interval based on the actual value
						scale := numVal / (numVal + bounds.AbsoluteError)

						ciLower := bounds.ConfidenceInterval.Lower * scale
						ciUpper := bounds.ConfidenceInterval.Upper * scale

						results[i][col+"_ci_low"] = ciLower
						results[i][col+"_ci_high"] = ciUpper
						results[i][col+"_rel_error"] = bounds.RelativeError
					}
				}
			}
		}
	}
}

// convertToFloat64 safely converts various numeric types to float64
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
	default:
		return 0, false
	}
}

// GenerateErrorSummary creates a human-readable error analysis
func (ee *ErrorEstimator) GenerateErrorSummary(bounds *StatisticalBounds) string {
	if bounds == nil {
		return "No error bounds available"
	}

	summary := fmt.Sprintf("Statistical Analysis: %.1f%% relative error", bounds.RelativeError*100)

	if bounds.ConfidenceInterval != nil {
		summary += fmt.Sprintf(" with %.0f%% confidence interval", bounds.ConfidenceInterval.Confidence*100)
		summary += fmt.Sprintf(" using %s method", bounds.ConfidenceInterval.Method)
	}

	if bounds.SampleSize > 0 && bounds.PopulationSize > 0 {
		samplingRate := float64(bounds.SampleSize) / float64(bounds.PopulationSize) * 100
		summary += fmt.Sprintf(" (%.1f%% sampling rate)", samplingRate)
	}

	if bounds.BiasCorrection != 0 {
		summary += fmt.Sprintf(", bias correction: %.3f", bounds.BiasCorrection)
	}

	return summary
}

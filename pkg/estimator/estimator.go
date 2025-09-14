package estimator

import (
    "math"
    "math/rand"
    "sort"
    "time"
)

// CIResult contains confidence interval metadata.
type CIResult struct {
    Estimate         float64 `json:"estimate"`
    StdError         float64 `json:"std_error"`
    ConfidenceLevel  float64 `json:"confidence_level"`
    Lower            float64 `json:"ci_low"`
    Upper            float64 `json:"ci_high"`
    SampleFraction   float64 `json:"sample_fraction"`
    RelativeError    float64 `json:"relative_error"`
}

// ZScore returns z for a two-sided confidence level (e.g., 0.95 -> ~1.96).
func ZScore(confidence float64) float64 {
    // Simple mapping for common levels; could replace with inverse CDF if needed.
    switch {
    case math.Abs(confidence-0.90) < 1e-9:
        return 1.6448536269514722
    case math.Abs(confidence-0.95) < 1e-9:
        return 1.959963984540054
    case math.Abs(confidence-0.99) < 1e-9:
        return 2.5758293035489004
    default:
        // default to 95%
        return 1.959963984540054
    }
}

// SumCI computes an analytic CI for a sum estimate scaled from a uniform sample.
// sum_hat = sum_sample / f ; Var(sum_hat) = Var(sum_sample) / f^2
// We approximate Var(sum_sample) via sample variance of contributing values.
func SumCI(sumSample float64, sampleValuesVariance float64, nSample int, f float64, confidence float64) CIResult {
    // sample variance of the sum is var(x) * nSample if independent; this is a simplification.
    varSumSample := sampleValuesVariance * float64(nSample)
    est := sumSample / f
    se := math.Sqrt(varSumSample) / f
    z := ZScore(confidence)
    low := est - z*se
    high := est + z*se
    rel := 0.0
    if est != 0 { rel = se / math.Abs(est) }
    return CIResult{Estimate: est, StdError: se, ConfidenceLevel: confidence, Lower: low, Upper: high, SampleFraction: f, RelativeError: rel}
}

// CountCI for COUNT(*) scaled from a uniform sample: count_hat = count_sample / f.
// Using binomial variance: Var(count_sample) ~= N*f*(1-f) with N unknown; we use count_hat as proxy for N.
func CountCI(countSample int64, f float64, confidence float64) CIResult {
    est := float64(countSample) / f
    // approximate variance of sample count ~ N*f*(1-f), use est for N.
    varSample := est * f * (1 - f)
    se := math.Sqrt(varSample) / f
    z := ZScore(confidence)
    low := est - z*se
    high := est + z*se
    rel := 0.0
    if est != 0 { rel = se / math.Abs(est) }
    return CIResult{Estimate: est, StdError: se, ConfidenceLevel: confidence, Lower: low, Upper: high, SampleFraction: f, RelativeError: rel}
}

// BootstrapCI computes bootstrap confidence intervals for a scaled estimate.
// values: sample values contributing to the estimate
// scaleFunc: function to compute the estimate from resampled values (e.g., sum, mean)
// scale: scaling factor (1/sampleFraction for totals)
// B: number of bootstrap iterations
// confidence: confidence level (e.g., 0.95)
func BootstrapCI(values []float64, scaleFunc func([]float64) float64, scale float64, B int, confidence float64) CIResult {
    if len(values) == 0 {
        return CIResult{}
    }
    
    rng := rand.New(rand.NewSource(time.Now().UnixNano()))
    n := len(values)
    
    // Original estimate
    originalEst := scaleFunc(values) * scale
    
    // Bootstrap resampling
    bootstrapEsts := make([]float64, B)
    for i := 0; i < B; i++ {
        // Resample with replacement
        resample := make([]float64, n)
        for j := 0; j < n; j++ {
            resample[j] = values[rng.Intn(n)]
        }
        bootstrapEsts[i] = scaleFunc(resample) * scale
    }
    
    // Sort bootstrap estimates
    sort.Float64s(bootstrapEsts)
    
    // Compute percentiles for CI
    alpha := 1.0 - confidence
    lowerIdx := int(math.Floor(float64(B) * alpha / 2.0))
    upperIdx := int(math.Ceil(float64(B) * (1.0 - alpha/2.0))) - 1
    
    if lowerIdx < 0 { lowerIdx = 0 }
    if upperIdx >= B { upperIdx = B - 1 }
    
    lower := bootstrapEsts[lowerIdx]
    upper := bootstrapEsts[upperIdx]
    
    // Compute standard error from bootstrap distribution
    mean := 0.0
    for _, est := range bootstrapEsts {
        mean += est
    }
    mean /= float64(B)
    
    variance := 0.0
    for _, est := range bootstrapEsts {
        variance += (est - mean) * (est - mean)
    }
    variance /= float64(B - 1)
    stdErr := math.Sqrt(variance)
    
    relErr := 0.0
    if originalEst != 0 {
        relErr = stdErr / math.Abs(originalEst)
    }
    
    return CIResult{
        Estimate:        originalEst,
        StdError:        stdErr,
        ConfidenceLevel: confidence,
        Lower:           lower,
        Upper:           upper,
        SampleFraction:  1.0 / scale, // approximate
        RelativeError:   relErr,
    }
}

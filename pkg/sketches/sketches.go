// Package sketches provides probabilistic data structures for approximate query processing
package sketches

// SketchType represents the type of sketch
type SketchType string

const (
    HyperLogLogType   SketchType = "hyperloglog"
    CountMinSketchType SketchType = "countmin"
)

// SketchInfo contains metadata about a sketch
type SketchInfo struct {
    Type       SketchType `json:"type"`
    Table      string     `json:"table"`
    Column     string     `json:"column,omitempty"`
    CreatedAt  int64      `json:"created_at"`
    Parameters map[string]interface{} `json:"parameters"`
}

// EstimateResult contains the result of a sketch query
type EstimateResult struct {
    Estimate        uint64  `json:"estimate"`
    ErrorBound      uint64  `json:"error_bound,omitempty"`
    Confidence      float64 `json:"confidence,omitempty"`
    Lower           uint64  `json:"ci_low,omitempty"`
    Upper           uint64  `json:"ci_high,omitempty"`
    SketchType      string  `json:"sketch_type"`
    SampleFraction  float64 `json:"sample_fraction,omitempty"`
}

// Sketch interface for all sketch types
type Sketch interface {
    // Serialize returns the sketch as bytes for storage
    Serialize() []byte
    
    // Type returns the sketch type
    Type() SketchType
}

// CardinalitySketch interface for cardinality estimation (HyperLogLog)
type CardinalitySketch interface {
    Sketch
    Add([]byte)
    AddString(string)
    Count() uint64
    StandardError() float64
    ConfidenceInterval(float64) (uint64, uint64)
}

// FrequencySketch interface for frequency estimation (Count-Min Sketch)
type FrequencySketch interface {
    Sketch
    Add([]byte, uint64)
    AddString(string, uint64)
    Query([]byte) uint64
    QueryString(string) uint64
    TotalCount() uint64
    ErrorBound() uint64
    Confidence() float64
}

// Ensure implementations satisfy interfaces
var _ CardinalitySketch = (*HyperLogLog)(nil)
var _ FrequencySketch = (*CountMinSketch)(nil)

// Type implementations
func (hll *HyperLogLog) Type() SketchType {
    return HyperLogLogType
}

func (cms *CountMinSketch) Type() SketchType {
    return CountMinSketchType
}
package sketches

import (
    "encoding/binary"
    "fmt"
    "hash/fnv"
    "math"
)

// CountMinSketch implements the Count-Min Sketch for frequency estimation
type CountMinSketch struct {
    table  [][]uint64 // count table[d][w]
    d      uint32     // number of hash functions (depth)
    w      uint32     // number of counters per hash (width)
    epsilon float64   // relative error bound
    delta   float64   // probability bound
    count   uint64    // total count of all items
}

// NewCountMinSketch creates a new Count-Min Sketch
// epsilon: relative error bound (e.g., 0.01 for 1% error)
// delta: probability bound (e.g., 0.01 for 99% confidence)
func NewCountMinSketch(epsilon, delta float64) *CountMinSketch {
    if epsilon <= 0 || epsilon >= 1 {
        epsilon = 0.01 // default 1% error
    }
    if delta <= 0 || delta >= 1 {
        delta = 0.01 // default 99% confidence
    }
    
    // Calculate optimal parameters
    w := uint32(math.Ceil(math.E / epsilon))
    d := uint32(math.Ceil(math.Log(1 / delta)))
    
    // Create table
    table := make([][]uint64, d)
    for i := range table {
        table[i] = make([]uint64, w)
    }
    
    return &CountMinSketch{
        table:   table,
        d:       d,
        w:       w,
        epsilon: epsilon,
        delta:   delta,
        count:   0,
    }
}

// Add increments the count for a key by delta
func (cms *CountMinSketch) Add(key []byte, delta uint64) {
    hashes := cms.hash(key)
    
    for i := uint32(0); i < cms.d; i++ {
        j := hashes[i] % cms.w
        cms.table[i][j] += delta
    }
    
    cms.count += delta
}

// AddString is a convenience method for string keys
func (cms *CountMinSketch) AddString(key string, delta uint64) {
    cms.Add([]byte(key), delta)
}

// Query estimates the count for a key
func (cms *CountMinSketch) Query(key []byte) uint64 {
    hashes := cms.hash(key)
    
    // Return minimum count across all hash functions
    minCount := ^uint64(0) // max uint64
    for i := uint32(0); i < cms.d; i++ {
        j := hashes[i] % cms.w
        if cms.table[i][j] < minCount {
            minCount = cms.table[i][j]
        }
    }
    
    return minCount
}

// QueryString is a convenience method for string keys
func (cms *CountMinSketch) QueryString(key string) uint64 {
    return cms.Query([]byte(key))
}

// TotalCount returns the total count of all items
func (cms *CountMinSketch) TotalCount() uint64 {
    return cms.count
}

// ErrorBound returns the theoretical error bound for estimates
func (cms *CountMinSketch) ErrorBound() uint64 {
    return uint64(cms.epsilon * float64(cms.count))
}

// Confidence returns the confidence level (1 - delta)
func (cms *CountMinSketch) Confidence() float64 {
    return 1.0 - cms.delta
}

// HeavyHitters returns keys with estimated count > threshold
// Note: This is a simplified version - production would need key tracking
func (cms *CountMinSketch) HeavyHitters(threshold uint64) []uint64 {
    var heavyHitters []uint64
    
    // For each cell in the table, if value > threshold, it might be a heavy hitter
    // This is an approximation - real implementation would track actual keys
    seen := make(map[uint64]bool)
    
    for i := uint32(0); i < cms.d; i++ {
        for j := uint32(0); j < cms.w; j++ {
            count := cms.table[i][j]
            if count > threshold && !seen[count] {
                heavyHitters = append(heavyHitters, count)
                seen[count] = true
            }
        }
    }
    
    return heavyHitters
}

// Merge combines this CMS with another CMS (must have same parameters)
func (cms *CountMinSketch) Merge(other *CountMinSketch) error {
    if cms.d != other.d || cms.w != other.w {
        return fmt.Errorf("cannot merge CMS with different parameters")
    }
    
    for i := uint32(0); i < cms.d; i++ {
        for j := uint32(0); j < cms.w; j++ {
            cms.table[i][j] += other.table[i][j]
        }
    }
    
    cms.count += other.count
    return nil
}

// Serialize returns the CMS state as bytes
func (cms *CountMinSketch) Serialize() []byte {
    // Header: d(4) + w(4) + epsilon(8) + delta(8) + count(8) = 32 bytes
    // Data: d * w * 8 bytes for uint64 values
    headerSize := 32
    dataSize := int(cms.d * cms.w * 8)
    data := make([]byte, headerSize+dataSize)
    
    // Write header
    binary.LittleEndian.PutUint32(data[0:4], cms.d)
    binary.LittleEndian.PutUint32(data[4:8], cms.w)
    binary.LittleEndian.PutUint64(data[8:16], math.Float64bits(cms.epsilon))
    binary.LittleEndian.PutUint64(data[16:24], math.Float64bits(cms.delta))
    binary.LittleEndian.PutUint64(data[24:32], cms.count)
    
    // Write table data
    offset := headerSize
    for i := uint32(0); i < cms.d; i++ {
        for j := uint32(0); j < cms.w; j++ {
            binary.LittleEndian.PutUint64(data[offset:offset+8], cms.table[i][j])
            offset += 8
        }
    }
    
    return data
}

// DeserializeCountMinSketch loads CMS state from bytes
func DeserializeCountMinSketch(data []byte) (*CountMinSketch, error) {
    if len(data) < 32 {
        return nil, fmt.Errorf("insufficient data for CMS deserialization")
    }
    
    // Read header
    d := binary.LittleEndian.Uint32(data[0:4])
    w := binary.LittleEndian.Uint32(data[4:8])
    epsilon := math.Float64frombits(binary.LittleEndian.Uint64(data[8:16]))
    delta := math.Float64frombits(binary.LittleEndian.Uint64(data[16:24]))
    count := binary.LittleEndian.Uint64(data[24:32])
    
    expectedSize := 32 + int(d*w*8)
    if len(data) != expectedSize {
        return nil, fmt.Errorf("data length mismatch: expected %d, got %d", expectedSize, len(data))
    }
    
    // Create CMS
    cms := &CountMinSketch{
        table:   make([][]uint64, d),
        d:       d,
        w:       w,
        epsilon: epsilon,
        delta:   delta,
        count:   count,
    }
    
    for i := range cms.table {
        cms.table[i] = make([]uint64, w)
    }
    
    // Read table data
    offset := 32
    for i := uint32(0); i < d; i++ {
        for j := uint32(0); j < w; j++ {
            cms.table[i][j] = binary.LittleEndian.Uint64(data[offset : offset+8])
            offset += 8
        }
    }
    
    return cms, nil
}

// hash generates d independent hash values for a key
func (cms *CountMinSketch) hash(key []byte) []uint32 {
    hashes := make([]uint32, cms.d)
    
    // Use FNV-1a as base hash
    h := fnv.New32a()
    
    // Generate independent hashes using double hashing
    for i := uint32(0); i < cms.d; i++ {
        h.Reset()
        h.Write(key)
        // Add salt based on row index
        salt := make([]byte, 4)
        binary.LittleEndian.PutUint32(salt, i)
        h.Write(salt)
        hashes[i] = h.Sum32()
    }
    
    return hashes
}
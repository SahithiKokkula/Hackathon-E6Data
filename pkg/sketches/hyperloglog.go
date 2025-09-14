package sketches

import (
    "encoding/binary"
    "fmt"
    "hash/fnv"
    "math"
)

// HyperLogLog implements the HyperLogLog algorithm for cardinality estimation
type HyperLogLog struct {
    registers []uint8
    b         uint8    // number of bits for register selection (m = 2^b)
    m         uint32   // number of registers
    alpha     float64  // bias correction constant
}

// NewHyperLogLog creates a new HyperLogLog with 2^b registers
// Standard values: b=10 (1024 registers), b=12 (4096 registers)
func NewHyperLogLog(b uint8) *HyperLogLog {
    if b < 4 || b > 16 {
        b = 10 // default to 1024 registers
    }
    
    m := uint32(1 << b)
    
    // Calculate alpha constant for bias correction
    var alpha float64
    switch {
    case m >= 128:
        alpha = 0.7213 / (1 + 1.079/float64(m))
    case m >= 64:
        alpha = 0.709
    case m >= 32:
        alpha = 0.697
    case m >= 16:
        alpha = 0.673
    default:
        alpha = 0.5
    }
    
    return &HyperLogLog{
        registers: make([]uint8, m),
        b:         b,
        m:         m,
        alpha:     alpha,
    }
}

// Add adds a value to the HyperLogLog
func (hll *HyperLogLog) Add(value []byte) {
    hash := hash64(value)
    
    // Use first b bits for register selection
    j := hash & ((1 << hll.b) - 1)
    
    // Use remaining bits for leading zero count
    w := hash >> hll.b
    
    // Count leading zeros in w + 1
    leadingZeros := uint8(1)
    for w > 0 && leadingZeros <= 64-hll.b {
        if w&1 == 1 {
            break
        }
        leadingZeros++
        w >>= 1
    }
    
    // Update register with maximum leading zero count
    if leadingZeros > hll.registers[j] {
        hll.registers[j] = leadingZeros
    }
}

// AddString is a convenience method for adding string values
func (hll *HyperLogLog) AddString(value string) {
    hll.Add([]byte(value))
}

// Count estimates the cardinality
func (hll *HyperLogLog) Count() uint64 {
    // Calculate raw estimate
    rawEstimate := hll.alpha * float64(hll.m*hll.m) / hll.harmonicMean()
    
    // Apply small range correction
    if rawEstimate <= 2.5*float64(hll.m) {
        zeros := hll.countZeros()
        if zeros != 0 {
            return uint64(float64(hll.m) * math.Log(float64(hll.m)/float64(zeros)))
        }
    }
    
    // Apply large range correction for 32-bit hash
    if rawEstimate <= (1.0/30.0)*(1<<32) {
        return uint64(rawEstimate)
    }
    
    return uint64(-1*(1<<32)*math.Log(1-rawEstimate/(1<<32)))
}

// StandardError returns the theoretical standard error for this HLL
func (hll *HyperLogLog) StandardError() float64 {
    return 1.04 / math.Sqrt(float64(hll.m))
}

// ConfidenceInterval returns approximate confidence bounds
func (hll *HyperLogLog) ConfidenceInterval(confidence float64) (uint64, uint64) {
    estimate := float64(hll.Count())
    stdErr := hll.StandardError() * estimate
    
    // Use normal approximation for large estimates
    var z float64
    switch {
    case math.Abs(confidence-0.90) < 1e-9:
        z = 1.645
    case math.Abs(confidence-0.95) < 1e-9:
        z = 1.96
    case math.Abs(confidence-0.99) < 1e-9:
        z = 2.576
    default:
        z = 1.96 // default to 95%
    }
    
    margin := z * stdErr
    lower := math.Max(0, estimate-margin)
    upper := estimate + margin
    
    return uint64(lower), uint64(upper)
}

// Merge combines this HLL with another HLL (must have same parameters)
func (hll *HyperLogLog) Merge(other *HyperLogLog) error {
    if hll.m != other.m || hll.b != other.b {
        return fmt.Errorf("cannot merge HLLs with different parameters")
    }
    
    for i := uint32(0); i < hll.m; i++ {
        if other.registers[i] > hll.registers[i] {
            hll.registers[i] = other.registers[i]
        }
    }
    
    return nil
}

// Serialize returns the HLL state as bytes
func (hll *HyperLogLog) Serialize() []byte {
    data := make([]byte, 5+len(hll.registers))
    data[0] = hll.b
    binary.LittleEndian.PutUint32(data[1:5], hll.m)
    copy(data[5:], hll.registers)
    return data
}

// Deserialize loads HLL state from bytes
func DeserializeHyperLogLog(data []byte) (*HyperLogLog, error) {
    if len(data) < 5 {
        return nil, fmt.Errorf("insufficient data for HLL deserialization")
    }
    
    b := data[0]
    m := binary.LittleEndian.Uint32(data[1:5])
    
    if len(data) != int(5+m) {
        return nil, fmt.Errorf("data length mismatch")
    }
    
    hll := NewHyperLogLog(b)
    copy(hll.registers, data[5:])
    
    return hll, nil
}

// Helper functions

func hash64(data []byte) uint64 {
    h := fnv.New64a()
    h.Write(data)
    return h.Sum64()
}

func (hll *HyperLogLog) harmonicMean() float64 {
    sum := 0.0
    for _, reg := range hll.registers {
        sum += math.Pow(2, -float64(reg))
    }
    return sum
}

func (hll *HyperLogLog) countZeros() uint32 {
    count := uint32(0)
    for _, reg := range hll.registers {
        if reg == 0 {
            count++
        }
    }
    return count
}
import React, { useEffect, useMemo, useState } from 'react'

type QueryResult = {
  status: string
  plan?: any
  result?: Array<Record<string, any>>
  meta?: Record<string, any>
  error?: string
  ml_optimization?: {
    strategy: string
    modified_sql: string
    original_sql: string
    confidence: number
    estimated_speedup: number
    estimated_error: number
    reasoning: string
    transformations: string[]
  }
}

type ErrorBarChartProps = {
  data: Array<Record<string, any>>
  xKey: string
  yKey: string
}

function ErrorBarChart({ data, xKey, yKey }: ErrorBarChartProps) {
  const ciLowKey = yKey + '_ci_low'
  const ciHighKey = yKey + '_ci_high'
  
  const maxVal = useMemo(() => {
    if (!data || data.length === 0) return 1;
    return Math.max(...data.map(row => {
      const high = row[ciHighKey]
      return typeof high === 'number' ? high : (row[yKey] as number || 0)
    }))
  }, [data, yKey, ciHighKey])
  
  return (
    <div style={{ marginTop: 16 }}>
      <h4>Error Bar Chart: {yKey} by {xKey}</h4>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 4, maxHeight: 300, overflowY: 'auto' }}>
        {data && data.length > 0 && data.slice(0, 10).map((row, i) => {
          const xVal = String(row[xKey] || `Row ${i+1}`)
          const yVal = Number(row[yKey]) || 0
          const ciLow = Number(row[ciLowKey]) || yVal
          const ciHigh = Number(row[ciHighKey]) || yVal
          const barWidth = Math.max(1, (yVal / maxVal) * 200)
          const errorLow = Math.max(0, (ciLow / maxVal) * 200)
          const errorHigh = Math.min(200, (ciHigh / maxVal) * 200)
          
          return (
            <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 12 }}>
              <div style={{ width: 80, textAlign: 'right', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                {xVal}
              </div>
              <div style={{ position: 'relative', height: 20, width: 220 }}>
                {/* Error bar background */}
                <div style={{
                  position: 'absolute',
                  left: errorLow,
                  width: errorHigh - errorLow,
                  height: 2,
                  backgroundColor: '#ffc107',
                  top: 9
                }} />
                {/* Value bar */}
                <div style={{
                  position: 'absolute',
                  width: barWidth,
                  height: 16,
                  backgroundColor: '#007bff',
                  top: 2,
                  borderRadius: 2
                }} />
                {/* CI markers */}
                <div style={{
                  position: 'absolute',
                  left: errorLow - 1,
                  width: 2,
                  height: 8,
                  backgroundColor: '#ffc107',
                  top: 6
                }} />
                <div style={{
                  position: 'absolute',
                  left: errorHigh - 1,
                  width: 2,
                  height: 8,
                  backgroundColor: '#ffc107',
                  top: 6
                }} />
              </div>
              <div style={{ fontSize: 10, color: '#666' }}>
                {yVal.toFixed(1)} [{ciLow.toFixed(1)}, {ciHigh.toFixed(1)}]
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

export default function App() {
  const [tables, setTables] = useState<string[]>([])
  const [sql, setSql] = useState<string>('SELECT country, SUM(amount) AS revenue FROM purchases GROUP BY country ORDER BY revenue DESC LIMIT 10;')
  const [maxRelError, setMaxRelError] = useState<number>(0.05)
  const [preferExact, setPreferExact] = useState<boolean>(false)
  const [loading, setLoading] = useState(false)
  const [resp, setResp] = useState<QueryResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetch('/tables')
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(d => {
        if (d && Array.isArray(d.tables)) {
          setTables(d.tables);
        } else {
          setTables([]);
        }
      })
      .catch(err => {
        console.error('Failed to fetch tables:', err);
        setTables([]);
      })
  }, [])

  const runQuery = async (opts?: { preferExact?: boolean; useMLOptimization?: boolean }) => {
    setLoading(true); setError(null)
    try {
      const r = await fetch('/query', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          sql, 
          max_rel_error: maxRelError, 
          prefer_exact: opts?.preferExact ?? preferExact, 
          use_ml_optimization: opts?.useMLOptimization ?? true,
          explain: false 
        })
      })
      const d: QueryResult = await r.json()
      if (!r.ok || d.status !== 'ok') throw new Error(d.error || 'Query failed')
      setResp(d)
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  const columns = useMemo(() => {
    const first = resp?.result?.[0];
    return first ? Object.keys(first).filter(k => !k.endsWith('_ci_low') && !k.endsWith('_ci_high') && !k.endsWith('_rel_error')) : []
  }, [resp])
  
  const hasErrorBars = useMemo(() => {
    const first = resp?.result?.[0];
    if (!first) return false
    return Object.keys(first).some(k => k.endsWith('_ci_low'))
  }, [resp])

  return (
    <div style={{ fontFamily: 'system-ui, sans-serif', padding: 16 }}>
      <h1>Approximate Query Engine</h1>
      <div style={{ marginBottom: 8 }}>
        <strong>Tables:</strong> {tables.join(', ') || '(none)'}
      </div>
      <textarea
        value={sql}
        onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => setSql(e.target.value)}
        rows={8}
        style={{ width: '100%', fontFamily: 'monospace' }}
      />
      <div style={{ display: 'flex', gap: 16, alignItems: 'center', marginTop: 8 }}>
        <label>
          max_rel_error: {" "}
          <input
            type="number"
            step="0.01"
            value={maxRelError}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
              const v = e.target.value
              const n = v === '' ? 0 : Number(v)
              setMaxRelError(isNaN(n) ? 0 : n)
            }}
          />
        </label>
        <label>
          <input
            type="checkbox"
            checked={preferExact}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setPreferExact(e.target.checked)}
          />
          {" "}prefer_exact
        </label>
  <button 
    onClick={() => runQuery({ useMLOptimization: true, preferExact: false })} 
    disabled={loading}
    style={{ backgroundColor: '#28a745', color: 'white', padding: '8px 16px', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
  >
    {loading ? 'Running…' : 'Run (ML Optimized)'}
  </button>
  <button 
    onClick={() => runQuery({ preferExact: true, useMLOptimization: false })} 
    disabled={loading}
    style={{ backgroundColor: '#6c757d', color: 'white', padding: '8px 16px', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
  >
    Run exact
  </button>
      </div>
      {error && <div style={{ color: 'red', marginTop: 8 }}>{error}</div>}
      {resp?.ml_optimization && (
        <div style={{ marginTop: 16 }}>
          <h3>ML Optimization</h3>
          <div style={{ background: '#e8f5e8', padding: 12, borderRadius: 4, border: '1px solid #4caf50' }}>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
              <div>
                <strong>Strategy:</strong> {resp.ml_optimization.strategy}<br/>
                <strong>Confidence:</strong> {(resp.ml_optimization.confidence * 100).toFixed(1)}%<br/>
                <strong>Est. Speedup:</strong> {resp.ml_optimization.estimated_speedup.toFixed(1)}x<br/>
                <strong>Est. Error:</strong> {(resp.ml_optimization.estimated_error * 100).toFixed(2)}%
              </div>
              <div>
                <strong>Reasoning:</strong><br/>
                <em style={{ fontSize: '0.9em' }}>{resp.ml_optimization.reasoning}</em>
              </div>
            </div>
            {resp.ml_optimization.transformations && resp.ml_optimization.transformations.length > 0 && (
              <div style={{ marginTop: 8 }}>
                <strong>Applied Transformations:</strong>
                <ul style={{ margin: '4px 0', paddingLeft: 20 }}>
                  {resp.ml_optimization.transformations.map((t, i) => (
                    <li key={i} style={{ fontSize: '0.9em' }}>{t}</li>
                  ))}
                </ul>
              </div>
            )}
            {resp.ml_optimization.modified_sql !== resp.ml_optimization.original_sql && (
              <details style={{ marginTop: 8 }}>
                <summary style={{ cursor: 'pointer', fontWeight: 'bold' }}>Modified SQL</summary>
                <pre style={{ background: '#f0f0f0', padding: 8, marginTop: 4, fontSize: '0.85em', overflowX: 'auto' }}>
                  {resp.ml_optimization.modified_sql}
                </pre>
              </details>
            )}
          </div>
        </div>
      )}
      {resp && (
        <div style={{ marginTop: 16 }}>
          <h3>Plan</h3>
          <pre style={{ background: '#f6f8fa', padding: 8, overflowX: 'auto' }}>{JSON.stringify(resp.plan, null, 2)}</pre>
          <h3>Meta</h3>
          <pre style={{ background: '#f6f8fa', padding: 8, overflowX: 'auto' }}>{JSON.stringify(resp.meta, null, 2)}</pre>
          <h3>Result ({resp.result?.length || 0} rows) {resp.result && resp.result.length > 100 && "(showing first 100 rows)"}</h3>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ borderCollapse: 'collapse', minWidth: '600px' }}>
              <thead>
                <tr>
                  {columns.map(c => <th key={c} style={{ border: '1px solid #ddd', padding: 4, textAlign: 'left' }}>{c}</th>)}
                </tr>
              </thead>
              <tbody>
                {resp.result?.slice(0, 100).map((row, i) => (
                  <tr key={i}>
                    {columns.map(c => {
                      const val = row[c]
                      const ciLow = row[c + '_ci_low']
                      const ciHigh = row[c + '_ci_high']
                      const relError = row[c + '_rel_error']
                      
                      let displayVal = String(val)
                      if (typeof val === 'number' && ciLow !== undefined && ciHigh !== undefined) {
                        displayVal = `${val.toFixed(2)} ± ${((ciHigh - ciLow) / 2).toFixed(2)}`
                        if (relError) {
                          displayVal += ` (${(relError * 100).toFixed(1)}%)`
                        }
                      }
                      
                      return (
                        <td key={c} style={{ border: '1px solid #eee', padding: 4 }}>
                          {displayVal}
                        </td>
                      )
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {hasErrorBars && resp.result && resp.result.length > 0 && (
            <ErrorBarChart 
              data={resp.result} 
              xKey={columns[0] || 'index'} 
              yKey={columns.find(c => resp.result && resp.result[0] && typeof resp.result[0][c] === 'number') || columns[1] || 'value'} 
            />
          )}
          {resp.meta?.confidence && (
            <div style={{ marginTop: 12 }}>
              <h4>Confidence (95%)</h4>
              <pre style={{ background: '#f6f8fa', padding: 8, overflowX: 'auto' }}>{JSON.stringify(resp.meta.confidence, null, 2)}</pre>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

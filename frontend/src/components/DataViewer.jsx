import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import axios from 'axios';
import { ArrowLeftRight, ChevronDown, RefreshCw, Table2, X } from 'lucide-react';

const API_BASE = "http://localhost:8001/api";
const EMPTY_DATA = { rows: [], columns: [] };

const DataViewer = ({ onClose, runId = null, embedded = false }) => {
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState(null);
  const [syntheticData, setSyntheticData] = useState(EMPTY_DATA);
  const [sourceData, setSourceData] = useState(EMPTY_DATA);
  const [view, setView] = useState('sideBySide');
  const [loading, setLoading] = useState(false);
  const [loadingTables, setLoadingTables] = useState(true);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const tablesRef = useRef([]);
  const selectedTableRef = useRef(null);

  useEffect(() => {
    tablesRef.current = tables;
  }, [tables]);

  useEffect(() => {
    selectedTableRef.current = selectedTable;
  }, [selectedTable]);

  const selectedMeta = useMemo(
    () => tables.find((table) => table.table_name === selectedTable) || null,
    [tables, selectedTable]
  );

  const loadTable = useCallback(async (tableName, tableMeta = null) => {
    if (!tableName) {
      return;
    }

    const resolvedMeta = tableMeta || tablesRef.current.find((table) => table.table_name === tableName);
    setSelectedTable(tableName);
    setLoading(true);
    setDropdownOpen(false);

    try {
      const [synRes, srcRes] = await Promise.all([
        resolvedMeta?.has_generated
          ? axios.get(`${API_BASE}/generated-data/${tableName}`, {
              params: resolvedMeta.generated_run_id ? { run_id: resolvedMeta.generated_run_id } : {},
            })
          : Promise.resolve({ data: EMPTY_DATA }),
        resolvedMeta?.has_source === false
          ? Promise.resolve({ data: EMPTY_DATA })
          : axios.get(`${API_BASE}/source-data/${tableName}`, {
              params: runId ? { run_id: runId } : {},
            }).catch(() => ({ data: EMPTY_DATA })),
      ]);
      setSyntheticData(synRes.data || EMPTY_DATA);
      setSourceData(srcRes.data || EMPTY_DATA);
    } catch (err) {
      console.error("Failed to load table data", err);
      setSyntheticData(EMPTY_DATA);
      setSourceData(EMPTY_DATA);
    } finally {
      setLoading(false);
    }
  }, [runId]);

  const fetchTables = useCallback(async () => {
    setLoadingTables(true);
    try {
      const res = await axios.get(`${API_BASE}/data/tables`, {
        params: runId ? { run_id: runId } : {},
      });
      const orderedTables = [...(res.data || [])].sort((left, right) => {
        if (left.has_generated !== right.has_generated) {
          return Number(right.has_generated) - Number(left.has_generated);
        }
        return left.table_name.localeCompare(right.table_name);
      });
      setTables(orderedTables);

      const currentSelection = selectedTableRef.current;
      const nextTable =
        currentSelection && orderedTables.some((table) => table.table_name === currentSelection)
          ? currentSelection
          : orderedTables.find((table) => table.has_generated)?.table_name || orderedTables[0]?.table_name;

      if (nextTable) {
        const nextMeta = orderedTables.find((table) => table.table_name === nextTable);
        await loadTable(nextTable, nextMeta);
      } else {
        setSelectedTable(null);
        setSyntheticData(EMPTY_DATA);
        setSourceData(EMPTY_DATA);
      }
    } catch (err) {
      console.error("Failed to fetch available tables", err);
      setTables([]);
    } finally {
      setLoadingTables(false);
    }
  }, [loadTable, runId]);

  useEffect(() => {
    fetchTables();
  }, [fetchTables]);

  const renderTable = (data, label, accentClass) => {
    if (!data || !data.rows || data.rows.length === 0) {
      return (
        <div className="rounded-3xl border border-dashed border-slate-200 bg-white px-6 py-14 text-center text-sm text-slate-500">
          No {label.toLowerCase()} available.
        </div>
      );
    }

    const cols = data.columns || Object.keys(data.rows[0]);
    return (
      <div className="min-w-0">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
          <h3 className={`text-sm font-semibold ${accentClass}`}>{label}</h3>
          <span className="text-xs text-slate-500">
            Showing {data.rows.length} of {data.total_rows || data.rows.length} rows
            {data.run_id && <span className="ml-2">| Run: {data.run_id.slice(0, 8)}</span>}
          </span>
        </div>
        <div className="max-h-[52vh] overflow-auto rounded-3xl border border-slate-200 bg-white">
          <table className="w-full text-xs">
            <thead className="sticky top-0 bg-slate-50">
              <tr>
                <th className="p-2 text-left font-medium text-slate-500">#</th>
                {cols.map((col) => (
                  <th key={col} className="whitespace-nowrap p-2 text-left font-medium text-slate-500">{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.rows.map((row, index) => (
                <tr key={index} className="border-t border-slate-100 hover:bg-slate-50">
                  <td className="p-2 text-slate-400">{index + 1}</td>
                  {cols.map((col) => (
                    <td key={col} className="max-w-[220px] truncate whitespace-nowrap p-2 font-mono text-slate-700">
                      {row[col] == null ? <span className="italic text-slate-400">null</span> : String(row[col])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  const content = (
    <div className="flex h-full flex-col overflow-hidden rounded-[28px] border border-slate-200 bg-white shadow-sm">
      <div className="flex flex-wrap items-start justify-between gap-4 border-b border-slate-200 px-6 py-5">
        <div className="flex items-start gap-4">
          <div className="rounded-2xl bg-sky-100 p-3 text-sky-600">
            <Table2 size={22} />
          </div>
          <div>
            <h2 className="text-xl font-semibold text-slate-900">Synthetic Data Viewer</h2>
            <p className="mt-1 text-sm text-slate-500">
              Compare source data with {runId ? `run ${runId.slice(0, 8)}` : 'available'} generated output.
            </p>
          </div>
        </div>
        {!embedded && (
          <button onClick={onClose} className="rounded-full border border-slate-200 p-2 text-slate-400 transition hover:text-slate-900">
            <X size={20} />
          </button>
        )}
      </div>

      <div className="flex flex-wrap items-center gap-4 border-b border-slate-200 bg-slate-50 p-4">
        <div className="relative">
          <button
            onClick={() => setDropdownOpen(!dropdownOpen)}
            className="flex min-w-[280px] items-center gap-2 rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm hover:border-slate-300"
          >
            <span className="font-mono text-slate-900">{selectedTable || 'Select table...'}</span>
            <ChevronDown size={14} className="ml-auto text-slate-400" />
          </button>
          {dropdownOpen && (
            <div className="absolute left-0 top-full z-10 mt-2 max-h-80 min-w-[300px] overflow-auto rounded-2xl border border-slate-200 bg-white shadow-xl">
              {tables.map((table) => (
                <button
                  key={table.table_name}
                  onClick={() => loadTable(table.table_name, table)}
                  className={`block w-full px-4 py-3 text-left text-sm hover:bg-slate-50 ${table.table_name === selectedTable ? 'bg-slate-50' : ''}`}
                >
                  <div className="font-mono text-slate-900">{table.table_name}</div>
                  <div className="mt-1 flex gap-2 text-[11px] text-slate-500">
                    <span className={table.has_source ? 'text-emerald-600' : 'text-slate-400'}>Source</span>
                    <span className={table.has_generated ? 'text-sky-600' : 'text-slate-400'}>Generated</span>
                    {table.tier && <span className="text-amber-600">{table.tier}</span>}
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>

        <div className="flex overflow-hidden rounded-full border border-slate-200 bg-white">
          <button
            onClick={() => setView('synthetic')}
            className={`px-4 py-2 text-xs font-medium ${view === 'synthetic' ? 'bg-slate-900 text-white' : 'text-slate-500 hover:text-slate-900'}`}
          >
            Synthetic
          </button>
          <button
            onClick={() => setView('source')}
            className={`px-4 py-2 text-xs font-medium ${view === 'source' ? 'bg-slate-900 text-white' : 'text-slate-500 hover:text-slate-900'}`}
          >
            Source
          </button>
          <button
            onClick={() => setView('sideBySide')}
            className={`flex items-center gap-1 px-4 py-2 text-xs font-medium ${view === 'sideBySide' ? 'bg-slate-900 text-white' : 'text-slate-500 hover:text-slate-900'}`}
          >
            <ArrowLeftRight size={12} />
            Compare
          </button>
        </div>

        <button
          onClick={fetchTables}
          className="flex items-center gap-2 rounded-full border border-slate-200 bg-white px-3 py-2 text-xs text-slate-600 hover:border-slate-300"
        >
          <RefreshCw size={12} className={loadingTables ? 'animate-spin' : ''} />
          Refresh
        </button>

        {selectedMeta && (
          <div className="ml-auto flex flex-wrap gap-2 text-xs">
            <span className={`rounded-full px-3 py-1 ${selectedMeta.has_source ? 'bg-emerald-100 text-emerald-700' : 'bg-slate-200 text-slate-500'}`}>
              Source {selectedMeta.source_row_count != null ? `${selectedMeta.source_row_count.toLocaleString()} rows` : 'unavailable'}
            </span>
            <span className={`rounded-full px-3 py-1 ${selectedMeta.has_generated ? 'bg-sky-100 text-sky-700' : 'bg-slate-200 text-slate-500'}`}>
              Generated {selectedMeta.generated_row_count != null ? `${selectedMeta.generated_row_count.toLocaleString()} rows` : 'not ready'}
            </span>
          </div>
        )}
      </div>

      <div className="flex-1 overflow-auto p-6">
        {loading || loadingTables ? (
          <div className="py-12 text-center text-slate-500">Loading data...</div>
        ) : tables.length === 0 ? (
          <div className="rounded-3xl border border-dashed border-slate-200 bg-white px-6 py-14 text-center text-sm text-slate-500">
            No tables available yet for this run.
          </div>
        ) : view === 'sideBySide' ? (
          <div className="grid gap-6 xl:grid-cols-2">
            {renderTable(sourceData, 'Source Data', 'text-emerald-700')}
            {renderTable(syntheticData, 'Synthetic Data', 'text-sky-700')}
          </div>
        ) : view === 'source' ? (
          renderTable(sourceData, 'Source Data', 'text-emerald-700')
        ) : (
          renderTable(syntheticData, 'Synthetic Data', 'text-sky-700')
        )}
      </div>
    </div>
  );

  if (embedded) {
    return content;
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
      <div className="h-[90vh] w-full max-w-[95vw]">{content}</div>
    </div>
  );
};

export default DataViewer;

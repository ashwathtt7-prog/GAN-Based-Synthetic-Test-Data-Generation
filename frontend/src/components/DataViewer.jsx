import React, { useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import { ArrowLeftRight, ChevronDown, RefreshCw, Table2, X } from 'lucide-react';

const API_BASE = "http://localhost:8001/api";

const EMPTY_DATA = { rows: [], columns: [] };

const DataViewer = ({ onClose }) => {
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState(null);
  const [syntheticData, setSyntheticData] = useState(EMPTY_DATA);
  const [sourceData, setSourceData] = useState(EMPTY_DATA);
  const [view, setView] = useState('sideBySide');
  const [loading, setLoading] = useState(false);
  const [loadingTables, setLoadingTables] = useState(true);
  const [dropdownOpen, setDropdownOpen] = useState(false);

  const selectedMeta = useMemo(
    () => tables.find((table) => table.table_name === selectedTable) || null,
    [tables, selectedTable]
  );

  const loadTable = async (tableName, tableMeta = null) => {
    if (!tableName) {
      return;
    }

    const resolvedMeta = tableMeta || tables.find((table) => table.table_name === tableName);
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
          : axios.get(`${API_BASE}/source-data/${tableName}`).catch(() => ({ data: EMPTY_DATA })),
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
  };

  const fetchTables = async () => {
    setLoadingTables(true);
    try {
      const res = await axios.get(`${API_BASE}/data/tables`);
      const orderedTables = [...res.data].sort((left, right) => {
        if (left.has_generated !== right.has_generated) {
          return Number(right.has_generated) - Number(left.has_generated);
        }
        return left.table_name.localeCompare(right.table_name);
      });
      setTables(orderedTables);

      const nextTable =
        selectedTable && orderedTables.some((table) => table.table_name === selectedTable)
          ? selectedTable
          : orderedTables.find((table) => table.has_generated)?.table_name || orderedTables[0]?.table_name;

      if (nextTable) {
        const nextMeta = orderedTables.find((table) => table.table_name === nextTable);
        loadTable(nextTable, nextMeta);
      }
    } catch (err) {
      console.error("Failed to fetch available tables", err);
      setTables([]);
    } finally {
      setLoadingTables(false);
    }
  };

  useEffect(() => {
    fetchTables();
  }, []);

  const renderTable = (data, label, accentClass) => {
    if (!data || !data.rows || data.rows.length === 0) {
      return <div className="text-gray-500 italic text-center py-10">No {label.toLowerCase()} available.</div>;
    }

    const cols = data.columns || Object.keys(data.rows[0]);
    return (
      <div className="min-w-0">
        <div className="flex justify-between items-center mb-2 gap-3">
          <h3 className={`text-sm font-semibold ${accentClass}`}>{label}</h3>
          <span className="text-xs text-gray-500 text-right">
            Showing {data.rows.length} of {data.total_rows || data.rows.length} rows
            {data.run_id && <span className="ml-2">| Run: {data.run_id.slice(0, 8)}</span>}
          </span>
        </div>
        <div className="overflow-auto max-h-[52vh] border border-gray-700 rounded-lg">
          <table className="w-full text-xs">
            <thead className="bg-gray-800 sticky top-0">
              <tr>
                <th className="text-left p-2 text-gray-500 font-medium">#</th>
                {cols.map((col) => (
                  <th key={col} className="text-left p-2 text-gray-400 font-medium whitespace-nowrap">{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.rows.map((row, index) => (
                <tr key={index} className="border-t border-gray-800 hover:bg-gray-800/50">
                  <td className="p-2 text-gray-600">{index + 1}</td>
                  {cols.map((col) => (
                    <td key={col} className="p-2 whitespace-nowrap max-w-[220px] truncate font-mono">
                      {row[col] == null ? <span className="text-gray-600 italic">null</span> : String(row[col])}
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

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-900 rounded-xl border border-gray-700 w-full max-w-[95vw] max-h-[90vh] overflow-hidden flex flex-col">
        <div className="flex justify-between items-center p-6 border-b border-gray-700">
          <div className="flex items-center gap-4">
            <Table2 className="text-blue-400" size={24} />
            <div>
              <h2 className="text-xl font-bold">Synthetic Data Viewer</h2>
              <p className="text-sm text-gray-400">Open ingested tables, inspect generated output, and compare both side by side.</p>
            </div>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-white">
            <X size={24} />
          </button>
        </div>

        <div className="flex items-center gap-4 p-4 border-b border-gray-700/50 bg-gray-800/30 flex-wrap">
          <div className="relative">
            <button
              onClick={() => setDropdownOpen(!dropdownOpen)}
              className="flex items-center gap-2 bg-gray-800 border border-gray-600 rounded-lg px-4 py-2 text-sm hover:border-gray-500 min-w-[260px]"
            >
              <span className="font-mono text-blue-300">{selectedTable || 'Select table...'}</span>
              <ChevronDown size={14} className="text-gray-400 ml-auto" />
            </button>
            {dropdownOpen && (
              <div className="absolute top-full left-0 mt-1 bg-gray-800 border border-gray-600 rounded-lg shadow-xl z-10 min-w-[280px] max-h-80 overflow-auto">
                {tables.map((table) => (
                  <button
                    key={table.table_name}
                    onClick={() => loadTable(table.table_name, table)}
                    className={`block w-full text-left px-4 py-3 text-sm hover:bg-gray-700 ${table.table_name === selectedTable ? 'bg-gray-700/70' : ''}`}
                  >
                    <div className="font-mono text-blue-300">{table.table_name}</div>
                    <div className="flex gap-2 mt-1 text-[11px] text-gray-400">
                      <span className={table.has_source ? 'text-emerald-400' : 'text-gray-500'}>Source</span>
                      <span className={table.has_generated ? 'text-blue-400' : 'text-gray-500'}>Generated</span>
                      {table.tier && <span className="text-amber-400">{table.tier}</span>}
                    </div>
                  </button>
                ))}
              </div>
            )}
          </div>

          <div className="flex bg-gray-800 rounded-lg border border-gray-700 overflow-hidden">
            <button
              onClick={() => setView('synthetic')}
              className={`px-4 py-2 text-xs font-medium transition-colors ${view === 'synthetic' ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-gray-200'}`}
            >
              Synthetic
            </button>
            <button
              onClick={() => setView('source')}
              className={`px-4 py-2 text-xs font-medium transition-colors ${view === 'source' ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-gray-200'}`}
            >
              Source
            </button>
            <button
              onClick={() => setView('sideBySide')}
              className={`px-4 py-2 text-xs font-medium transition-colors flex items-center gap-1 ${view === 'sideBySide' ? 'bg-blue-600 text-white' : 'text-gray-400 hover:text-gray-200'}`}
            >
              <ArrowLeftRight size={12} />
              Compare
            </button>
          </div>

          <button
            onClick={fetchTables}
            className="flex items-center gap-2 bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-xs text-gray-300 hover:border-gray-500"
          >
            <RefreshCw size={12} className={loadingTables ? 'animate-spin' : ''} />
            Refresh
          </button>

          {selectedMeta && (
            <div className="ml-auto flex gap-2 flex-wrap text-xs">
              <span className={`px-2.5 py-1 rounded-full ${selectedMeta.has_source ? 'bg-emerald-500/15 text-emerald-300' : 'bg-gray-700 text-gray-400'}`}>
                Source {selectedMeta.source_row_count != null ? `${selectedMeta.source_row_count.toLocaleString()} rows` : 'unavailable'}
              </span>
              <span className={`px-2.5 py-1 rounded-full ${selectedMeta.has_generated ? 'bg-blue-500/15 text-blue-300' : 'bg-gray-700 text-gray-400'}`}>
                Generated {selectedMeta.generated_row_count != null ? `${selectedMeta.generated_row_count.toLocaleString()} rows` : 'not ready'}
              </span>
            </div>
          )}
        </div>

        <div className="overflow-auto flex-1 p-6">
          {loading || loadingTables ? (
            <div className="text-center py-12 text-gray-400">Loading data...</div>
          ) : tables.length === 0 ? (
            <div className="text-center py-12 text-gray-500">No tables available yet. Ingest a source database or run the pipeline first.</div>
          ) : view === 'sideBySide' ? (
            <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
              {renderTable(sourceData, 'Source Data', 'text-emerald-300')}
              {renderTable(syntheticData, 'Synthetic Data', 'text-blue-300')}
            </div>
          ) : view === 'source' ? (
            renderTable(sourceData, 'Source Data', 'text-emerald-300')
          ) : (
            renderTable(syntheticData, 'Synthetic Data', 'text-blue-300')
          )}
        </div>
      </div>
    </div>
  );
};

export default DataViewer;

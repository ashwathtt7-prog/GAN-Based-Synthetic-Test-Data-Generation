import React, { useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import {
  ArrowRightLeft,
  CopyPlus,
  GitBranchPlus,
  RefreshCw,
  Search,
} from 'lucide-react';

const API_HOST =
  typeof window !== 'undefined' && window.location?.hostname
    ? window.location.hostname
    : '127.0.0.1';
const API_BASE = `http://${API_HOST}:8001/api`;

const formatValue = (value) => {
  if (value == null) return 'null';
  if (typeof value === 'object') return JSON.stringify(value);
  const text = String(value);
  return text.length > 140 ? `${text.slice(0, 137)}...` : text;
};

const FailedCasePanel = ({ sourceName, embedded = false, onSelectTable = null }) => {
  const [tableOptions, setTableOptions] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [selectedIdColumn, setSelectedIdColumn] = useState('');
  const [valueOptions, setValueOptions] = useState([]);
  const [selectedIdValue, setSelectedIdValue] = useState('');
  const [loading, setLoading] = useState(false);
  const [valueLoading, setValueLoading] = useState(false);
  const [traceData, setTraceData] = useState(null);
  const [syntheticCase, setSyntheticCase] = useState(null);
  const [error, setError] = useState('');

  useEffect(() => {
    const fetchTables = async () => {
      if (!sourceName) return;
      setLoading(true);
      setError('');
      try {
        const res = await axios.get(`${API_BASE}/failed-cases/tables`, {
          params: { source_name: sourceName },
        });
        const tables = res.data?.tables || [];
        setTableOptions(tables);
        if (tables.length > 0) {
          setSelectedTable((current) => current || tables[0].table_name);
          setSelectedIdColumn((current) => current || tables[0].id_column);
        }
      } catch (err) {
        setError(err?.response?.data?.detail || err.message || 'Failed to load case tables');
      } finally {
        setLoading(false);
      }
    };

    fetchTables();
  }, [sourceName]);

  useEffect(() => {
    const active = tableOptions.find((item) => item.table_name === selectedTable);
    if (active) {
      setSelectedIdColumn((current) =>
        active.id_columns?.includes(current) ? current : active.id_column
      );
    }
    setSelectedIdValue('');
    setValueOptions([]);
    setTraceData(null);
    setSyntheticCase(null);
  }, [selectedTable, tableOptions]);

  useEffect(() => {
    const fetchValues = async () => {
      if (!sourceName || !selectedTable || !selectedIdColumn) return;
      setValueLoading(true);
      try {
        const res = await axios.get(`${API_BASE}/failed-cases/values`, {
          params: {
            source_name: sourceName,
            table_name: selectedTable,
            id_column: selectedIdColumn,
          },
        });
        const values = res.data?.values || [];
        setValueOptions(values);
        if (values.length > 0) {
          setSelectedIdValue((current) => current || values[0].value);
        }
      } catch (err) {
        setError(err?.response?.data?.detail || err.message || 'Failed to load ID options');
      } finally {
        setValueLoading(false);
      }
    };

    fetchValues();
  }, [sourceName, selectedTable, selectedIdColumn]);

  const activeTableMeta = useMemo(
    () => tableOptions.find((item) => item.table_name === selectedTable) || null,
    [tableOptions, selectedTable]
  );

  const traceCase = async () => {
    if (!selectedTable || !selectedIdColumn || !selectedIdValue) return;
    setLoading(true);
    setError('');
    try {
      const res = await axios.get(`${API_BASE}/failed-cases/trace`, {
        params: {
          source_name: sourceName,
          table_name: selectedTable,
          id_column: selectedIdColumn,
          id_value: selectedIdValue,
        },
      });
      setTraceData(res.data);
      setSyntheticCase(null);
      if (onSelectTable) onSelectTable(selectedTable);
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || 'Failed to trace case');
    } finally {
      setLoading(false);
    }
  };

  const generateScenario = async () => {
    if (!selectedTable || !selectedIdColumn || !selectedIdValue) return;
    setLoading(true);
    setError('');
    try {
      const res = await axios.post(`${API_BASE}/failed-cases/generate`, {
        source_name: sourceName,
        table_name: selectedTable,
        id_column: selectedIdColumn,
        id_value: selectedIdValue,
      });
      setSyntheticCase(res.data);
      if (!traceData) {
        setTraceData({
          source_name: res.data?.source_name,
          root: res.data?.root,
          tables: res.data?.tables || [],
          links: res.data?.links || [],
        });
      }
    } catch (err) {
      setError(err?.response?.data?.detail || err.message || 'Failed to generate scenario');
    } finally {
      setLoading(false);
    }
  };

  const tablesToRender = syntheticCase?.tables || traceData?.tables || [];

  const content = (
    <div className="space-y-6">
      <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="flex items-start gap-4">
            <div className="rounded-2xl bg-sky-100 p-3 text-sky-700">
              <GitBranchPlus size={22} />
            </div>
            <div>
              <h2 className="text-xl font-semibold text-slate-900">Failed Case Scenarios</h2>
              <p className="mt-1 max-w-3xl text-sm text-slate-500">
                Pick a root order ID or other business key, trace the related rows across tables,
                and generate a masked synthetic replica bundle for that exact failure scenario.
              </p>
            </div>
          </div>
          <button
            onClick={traceCase}
            className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white px-4 py-2 text-xs font-medium text-slate-700 hover:bg-slate-50"
          >
            <RefreshCw size={12} className={loading ? 'animate-spin' : ''} />
            Refresh trace
          </button>
        </div>

        <div className="mt-6 grid gap-4 lg:grid-cols-3">
          <div>
            <label className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Root table</label>
            <select
              value={selectedTable}
              onChange={(e) => setSelectedTable(e.target.value)}
              className="mt-2 w-full rounded-2xl border border-slate-200 bg-white p-3 text-sm text-slate-700"
            >
              {loading && tableOptions.length === 0 && <option value="">Loading tables...</option>}
              {tableOptions.map((item) => (
                <option key={item.table_name} value={item.table_name}>
                  {item.label}
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Identifier column</label>
            <select
              value={selectedIdColumn}
              onChange={(e) => setSelectedIdColumn(e.target.value)}
              className="mt-2 w-full rounded-2xl border border-slate-200 bg-white p-3 text-sm text-slate-700"
            >
              {(activeTableMeta?.id_columns || []).map((column) => (
                <option key={column} value={column}>
                  {column}
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Identifier value</label>
            <select
              value={selectedIdValue}
              onChange={(e) => setSelectedIdValue(e.target.value)}
              className="mt-2 w-full rounded-2xl border border-slate-200 bg-white p-3 text-sm text-slate-700"
            >
              {valueLoading && valueOptions.length === 0 && <option value="">Loading values...</option>}
              {valueOptions.map((item) => (
                <option key={item.value} value={item.value}>
                  {item.label}
                </option>
              ))}
            </select>
          </div>
        </div>

        {activeTableMeta && (
          <p className="mt-3 text-xs text-slate-500">
            Dropdown values come from <span className="font-mono text-slate-700">{activeTableMeta.table_name}</span>,
            keyed by <span className="font-mono text-slate-700">{selectedIdColumn}</span>.
          </p>
        )}

        <div className="mt-5 flex flex-wrap items-center gap-3">
          <button
            onClick={traceCase}
            disabled={loading || !selectedIdValue}
            className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-60"
          >
            <Search size={14} />
            Trace failed case
          </button>
          <button
            onClick={generateScenario}
            disabled={loading || !selectedIdValue}
            className="inline-flex items-center gap-2 rounded-full bg-slate-900 px-4 py-2 text-sm font-semibold text-white hover:bg-slate-800 disabled:opacity-60"
          >
            <CopyPlus size={14} />
            Generate synthetic scenario
          </button>
          {syntheticCase?.scenario_id && (
            <span className="rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-xs font-mono text-emerald-700">
              Scenario {syntheticCase.scenario_id.slice(0, 8)}
            </span>
          )}
        </div>
      </div>

      {error && (
        <div className="rounded-2xl border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700">
          {error}
        </div>
      )}

      {traceData?.root && (
        <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
          <div className="flex flex-wrap items-center gap-3">
            <h3 className="text-lg font-semibold text-slate-900">Root case</h3>
            <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-mono text-slate-600">
              {traceData.root.table_name}.{traceData.root.id_column} = {traceData.root.id_value}
            </span>
          </div>
          <div className="mt-4 overflow-auto rounded-2xl border border-slate-200">
            <table className="w-full text-xs">
              <tbody>
                {Object.entries(traceData.root.rows?.[0] || {}).map(([key, value]) => (
                  <tr key={key} className="border-t border-slate-100">
                    <td className="w-56 bg-slate-50 p-2 font-mono text-slate-500">{key}</td>
                    <td className="p-2 font-mono text-slate-700">{formatValue(value)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {traceData?.links?.length > 0 && (
        <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
          <h3 className="text-lg font-semibold text-slate-900">Relationship branches</h3>
          <div className="mt-4 space-y-2">
            {traceData.links.map((link, idx) => (
              <div key={`${link.from_table}-${link.to_table}-${idx}`} className="flex flex-wrap items-center gap-2 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-xs text-amber-900">
                <span className="font-mono font-semibold">{link.from_table}</span>
                <ArrowRightLeft size={12} />
                <span className="font-mono font-semibold">{link.to_table}</span>
                <span>
                  via <span className="font-mono">{link.via_source_column}</span> / <span className="font-mono">{link.via_target_column}</span>
                </span>
                <span className="rounded-full bg-amber-200 px-2 py-0.5 font-semibold">
                  {link.row_count} related row{link.row_count === 1 ? '' : 's'}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="grid gap-4 xl:grid-cols-2">
        {tablesToRender.map((table) => (
          <div key={table.table_name} className="overflow-hidden rounded-[28px] border border-slate-200 bg-white shadow-sm">
            <div className="border-b border-slate-200 px-5 py-4">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                  <h4 className="font-mono text-base text-slate-900">{table.table_name}</h4>
                  <p className="text-xs text-slate-500">{table.row_count} row{table.row_count === 1 ? '' : 's'} in this scenario</p>
                </div>
                <span className={`rounded-full px-3 py-1 text-[11px] font-semibold ${syntheticCase ? 'bg-emerald-100 text-emerald-700' : 'bg-slate-100 text-slate-600'}`}>
                  {syntheticCase ? 'Synthetic replica' : 'Source trace'}
                </span>
              </div>
            </div>
            <div className="max-h-[26rem] overflow-auto">
              <table className="w-full text-[11px]">
                <thead className="sticky top-0 bg-slate-50 text-slate-600">
                  <tr>
                    {Object.keys(table.rows?.[0] || {}).map((column) => (
                      <th key={column} className="whitespace-nowrap border-b border-slate-200 px-3 py-2 text-left font-semibold">
                        {column}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {(table.rows || []).map((row, rowIndex) => (
                    <tr key={rowIndex} className="border-b border-slate-100">
                      {Object.keys(table.rows?.[0] || {}).map((column) => (
                        <td key={column} className="whitespace-nowrap px-3 py-2 font-mono text-slate-700">
                          {formatValue(row[column])}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ))}
      </div>
    </div>
  );

  if (embedded) return content;
  return <div className="p-4">{content}</div>;
};

export default FailedCasePanel;

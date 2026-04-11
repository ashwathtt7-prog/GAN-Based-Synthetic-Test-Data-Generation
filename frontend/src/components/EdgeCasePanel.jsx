import React, { useCallback, useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import {
  AlertTriangle,
  ChevronRight,
  Link2,
  RefreshCw,
  ShieldAlert,
  X,
} from 'lucide-react';

const API_BASE = 'http://localhost:8001/api';

const SEVERITY_STYLES = {
  critical: 'bg-rose-100 text-rose-700 border-rose-200',
  high: 'bg-amber-100 text-amber-700 border-amber-200',
  medium: 'bg-sky-100 text-sky-700 border-sky-200',
};

const formatValue = (value) => {
  if (value == null) return 'null';
  if (typeof value === 'object') return JSON.stringify(value);
  const s = String(value);
  return s.length > 120 ? `${s.slice(0, 117)}…` : s;
};

const EdgeCasePanel = ({
  runId = null,
  sourceName = null,
  selectedTable = null,
  onSelectTable = null,
  embedded = false,
  onClose = null,
}) => {
  const [report, setReport] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [focusTable, setFocusTable] = useState(selectedTable);
  const [expandedDefectId, setExpandedDefectId] = useState(null);

  useEffect(() => {
    setFocusTable(selectedTable);
  }, [selectedTable]);

  const fetchReport = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await axios.get(`${API_BASE}/edge-cases/production-defects`, {
        params: { run_id: runId || undefined },
      });
      setReport(res.data || null);
    } catch (err) {
      console.error('Failed to load production-defect report', err);
      setError(err?.response?.data?.detail || err.message || 'Unknown error');
      setReport(null);
    } finally {
      setLoading(false);
    }
  }, [runId]);

  useEffect(() => {
    fetchReport();
  }, [fetchReport]);

  const tables = useMemo(() => report?.tables || [], [report]);

  const activeTable = useMemo(() => {
    if (!tables.length) return null;
    if (focusTable) {
      const match = tables.find(
        (t) => (t.table_name || '').toUpperCase() === focusTable.toUpperCase()
      );
      if (match) return match;
    }
    return tables[0];
  }, [tables, focusTable]);

  const handleSelectTable = (tableName) => {
    setFocusTable(tableName);
    setExpandedDefectId(null);
    if (onSelectTable) onSelectTable(tableName);
  };

  const content = (
    <div className="flex h-full flex-col overflow-hidden rounded-[28px] border border-slate-200 bg-white shadow-sm">
      <div className="flex flex-wrap items-start justify-between gap-4 border-b border-slate-200 px-6 py-5">
        <div className="flex items-start gap-4">
          <div className="rounded-2xl bg-rose-100 p-3 text-rose-600">
            <ShieldAlert size={22} />
          </div>
          <div>
            <h2 className="text-xl font-semibold text-slate-900">
              Production Edge Cases
            </h2>
            <p className="mt-1 max-w-2xl text-sm text-slate-500">
              Real rows detected in the source database that a production
              validator would reject — bad emails, negative balances,
              dangling foreign keys, impossible dates. Every value shown
              is the actual value read from the source, and each defect
              lists downstream rows linked via declared foreign keys.
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={fetchReport}
            className="flex items-center gap-2 rounded-full border border-slate-200 bg-white px-3 py-2 text-xs text-slate-600 hover:border-slate-300"
          >
            <RefreshCw size={12} className={loading ? 'animate-spin' : ''} />
            Refresh
          </button>
          {!embedded && onClose && (
            <button
              onClick={onClose}
              className="rounded-full border border-slate-200 p-2 text-slate-400 transition hover:text-slate-900"
            >
              <X size={20} />
            </button>
          )}
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-3 border-b border-slate-200 bg-slate-50 px-6 py-4">
        <span className="rounded-full border border-rose-200 bg-rose-50 px-3 py-1 text-xs font-semibold text-rose-700">
          {report?.total_defects ?? 0} real defects detected
        </span>
        <span className="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs text-slate-600">
          {tables.length} table{tables.length === 1 ? '' : 's'} affected
        </span>
        {report?.run_id && (
          <span className="rounded-full border border-slate-200 bg-white px-3 py-1 font-mono text-xs text-slate-500">
            Run {report.run_id.slice(0, 8)}
          </span>
        )}
        {sourceName && (
          <span className="rounded-full border border-slate-200 bg-white px-3 py-1 text-xs text-slate-500">
            Source: {sourceName}
          </span>
        )}
      </div>

      <div className="flex flex-1 overflow-hidden">
        {/* Left pane: list of tables with defects */}
        <aside className="w-72 shrink-0 overflow-y-auto border-r border-slate-200 bg-slate-50/60 p-4">
          <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
            Tables
          </div>
          {loading && !tables.length ? (
            <div className="text-xs text-slate-500">Loading defects…</div>
          ) : tables.length === 0 ? (
            <div className="rounded-xl border border-dashed border-slate-200 bg-white p-4 text-xs text-slate-500">
              {report?.message ||
                'No defects yet. Run the pipeline first and come back here once Phase 7.5 completes.'}
            </div>
          ) : (
            <ul className="space-y-2">
              {tables.map((table) => {
                const isActive =
                  activeTable &&
                  activeTable.table_name === table.table_name;
                return (
                  <li key={table.table_name}>
                    <button
                      onClick={() => handleSelectTable(table.table_name)}
                      className={`flex w-full items-center justify-between rounded-xl border px-3 py-2 text-left text-xs transition ${
                        isActive
                          ? 'border-rose-300 bg-white text-rose-700 shadow-sm'
                          : 'border-transparent bg-white/60 text-slate-600 hover:border-slate-200 hover:bg-white'
                      }`}
                    >
                      <span className="font-mono">{table.table_name}</span>
                      <span className="rounded-full bg-rose-100 px-2 py-0.5 font-semibold text-rose-700">
                        {table.defect_count}
                      </span>
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
        </aside>

        {/* Right pane: defects for the active table */}
        <section className="flex-1 overflow-y-auto p-6">
          {error ? (
            <div className="flex items-center gap-3 rounded-2xl border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700">
              <AlertTriangle size={18} />
              <span>Failed to load defect report: {error}</span>
            </div>
          ) : !activeTable ? (
            <div className="rounded-3xl border border-dashed border-slate-200 bg-white px-6 py-14 text-center text-sm text-slate-500">
              {loading
                ? 'Loading production-defect report…'
                : 'No defects to display yet. They appear here as soon as the pipeline reaches Phase 7.5.'}
            </div>
          ) : (
            <div className="space-y-5">
              <div>
                <h3 className="font-mono text-lg text-slate-900">
                  {activeTable.table_name}
                </h3>
                <p className="text-xs text-slate-500">
                  {activeTable.defect_count} defect rows detected across{' '}
                  {activeTable.total_rows_considered} source rows scanned.
                </p>
              </div>

              <ul className="space-y-3">
                {activeTable.defects.map((defect) => {
                  const isOpen = expandedDefectId === defect.defect_id;
                  const severityClass =
                    SEVERITY_STYLES[defect.severity] || SEVERITY_STYLES.medium;
                  return (
                    <li
                      key={defect.defect_id}
                      className="overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm"
                    >
                      <button
                        onClick={() =>
                          setExpandedDefectId(isOpen ? null : defect.defect_id)
                        }
                        className="flex w-full items-start justify-between gap-4 px-5 py-4 text-left transition hover:bg-slate-50"
                      >
                        <div className="min-w-0 flex-1">
                          <div className="flex flex-wrap items-center gap-2">
                            <span className="font-mono text-xs text-slate-400">
                              {defect.defect_id}
                            </span>
                            <span
                              className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${severityClass}`}
                            >
                              {defect.severity}
                            </span>
                            <span className="rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-medium text-slate-600">
                              {defect.defect_type}
                            </span>
                            {defect.impacted_tables?.length > 0 && (
                              <span className="flex items-center gap-1 rounded-full bg-amber-50 px-2 py-0.5 text-[10px] font-semibold text-amber-700">
                                <Link2 size={10} />
                                {defect.impacted_tables.reduce(
                                  (sum, imp) => sum + (imp.row_count || 0),
                                  0
                                )}{' '}
                                linked rows
                              </span>
                            )}
                          </div>
                          <p className="mt-2 text-sm font-semibold text-slate-900">
                            Column{' '}
                            <span className="font-mono text-rose-700">
                              {defect.column}
                            </span>{' '}
                            — row PK{' '}
                            <span className="font-mono text-slate-700">
                              {formatValue(defect.row_index)}
                            </span>
                          </p>
                          <p className="mt-1 text-xs text-slate-600">
                            {defect.prod_failure_reason}
                          </p>
                        </div>
                        <ChevronRight
                          size={16}
                          className={`mt-1 shrink-0 text-slate-400 transition ${
                            isOpen ? 'rotate-90' : ''
                          }`}
                        />
                      </button>

                      {isOpen && (
                        <div className="border-t border-slate-100 bg-slate-50/60 px-5 py-4">
                          <div>
                            <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                              Detected value (from source database)
                            </div>
                            <div className="mt-1 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 font-mono text-xs text-rose-700">
                              {formatValue(defect.original_value)}
                            </div>
                          </div>

                          <div className="mt-4">
                            <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                              Full source row
                            </div>
                            <div className="mt-1 max-h-48 overflow-auto rounded-lg border border-slate-200 bg-white">
                              <table className="w-full text-[11px]">
                                <tbody>
                                  {Object.entries(defect.example_row).map(
                                    ([k, v]) => (
                                      <tr
                                        key={k}
                                        className={`border-t border-slate-100 ${
                                          k === defect.column ? 'bg-rose-50' : ''
                                        }`}
                                      >
                                        <td className="w-48 p-2 font-mono text-slate-500">
                                          {k}
                                        </td>
                                        <td className="p-2 font-mono text-slate-700">
                                          {formatValue(v)}
                                        </td>
                                      </tr>
                                    )
                                  )}
                                </tbody>
                              </table>
                            </div>
                          </div>

                          {defect.impacted_tables?.length > 0 && (
                            <div className="mt-5">
                              <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-wide text-amber-700">
                                <Link2 size={12} />
                                Cross-table impact
                              </div>
                              <div className="mt-2 space-y-3">
                                {defect.impacted_tables.map((imp, idx) => (
                                  <div
                                    key={`${imp.table}-${idx}`}
                                    className="rounded-xl border border-amber-200 bg-amber-50/50 p-3"
                                  >
                                    <div className="flex flex-wrap items-center gap-2 text-xs">
                                      <button
                                        onClick={() => handleSelectTable(imp.table)}
                                        className="font-mono font-semibold text-amber-800 hover:underline"
                                      >
                                        {imp.table}
                                      </button>
                                      <span className="text-slate-500">
                                        via{' '}
                                        <span className="font-mono text-slate-700">
                                          {imp.via_column}
                                        </span>{' '}
                                        →{' '}
                                        <span className="font-mono text-slate-700">
                                          {imp.parent_key}
                                        </span>
                                      </span>
                                      <span className="rounded-full bg-amber-200 px-2 py-0.5 text-[10px] font-semibold text-amber-800">
                                        {imp.row_count} linked row
                                        {imp.row_count === 1 ? '' : 's'}
                                      </span>
                                    </div>
                                    {imp.rows?.length > 0 && (
                                      <div className="mt-2 max-h-40 overflow-auto rounded-lg border border-amber-200 bg-white">
                                        <table className="w-full text-[10px]">
                                          <thead className="bg-amber-100/70 text-amber-900">
                                            <tr>
                                              {Object.keys(imp.rows[0]).map(
                                                (col) => (
                                                  <th
                                                    key={col}
                                                    className="whitespace-nowrap p-2 text-left font-semibold"
                                                  >
                                                    {col}
                                                  </th>
                                                )
                                              )}
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {imp.rows.map((row, rIdx) => (
                                              <tr
                                                key={rIdx}
                                                className="border-t border-amber-100"
                                              >
                                                {Object.keys(imp.rows[0]).map(
                                                  (col) => (
                                                    <td
                                                      key={col}
                                                      className={`whitespace-nowrap p-2 font-mono ${
                                                        col === imp.via_column
                                                          ? 'bg-amber-50 text-amber-800'
                                                          : 'text-slate-700'
                                                      }`}
                                                    >
                                                      {formatValue(row[col])}
                                                    </td>
                                                  )
                                                )}
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      </div>
                                    )}
                                  </div>
                                ))}
                              </div>
                            </div>
                          )}
                        </div>
                      )}
                    </li>
                  );
                })}
              </ul>
            </div>
          )}
        </section>
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

export default EdgeCasePanel;

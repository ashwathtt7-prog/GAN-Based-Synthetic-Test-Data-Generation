import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Brain, ChevronDown, ChevronRight, Filter, X } from 'lucide-react';

const API_BASE = "http://localhost:8001/api";

const PII_COLORS = {
  none: 'bg-slate-100 text-slate-600 border-slate-200',
  sensitive_business: 'bg-rose-100 text-rose-700 border-rose-200',
  uncertain: 'bg-amber-100 text-amber-700 border-amber-200',
  not_pii: 'bg-slate-100 text-slate-600 border-slate-200',
};

const MASKING_COLORS = {
  passthrough: 'bg-emerald-100 text-emerald-700 border-emerald-200',
  suppress: 'bg-rose-100 text-rose-700 border-rose-200',
  substitute_realistic: 'bg-sky-100 text-sky-700 border-sky-200',
  format_preserving: 'bg-indigo-100 text-indigo-700 border-indigo-200',
  generalise: 'bg-amber-100 text-amber-700 border-amber-200',
};

const ReasoningPanel = ({ onClose, embedded = false }) => {
  const [policies, setPolicies] = useState([]);
  const [filterTable, setFilterTable] = useState('all');
  const [filterPii, setFilterPii] = useState('all');
  const [expandedRow, setExpandedRow] = useState(null);
  const [tables, setTables] = useState([]);

  useEffect(() => {
    const fetchPolicies = async () => {
      try {
        const res = await axios.get(`${API_BASE}/policies`);
        setPolicies(res.data || []);
        const uniqueTables = [...new Set((res.data || []).map(p => p.table_name))];
        setTables(uniqueTables);
      } catch (err) {
        console.error("Failed to fetch policies", err);
      }
    };
    fetchPolicies();
  }, []);

  const filtered = policies.filter(p => {
    if (filterTable !== 'all' && p.table_name !== filterTable) return false;
    if (filterPii === 'pii' && (p.pii_classification === 'none' || p.pii_classification === 'not_pii')) return false;
    if (filterPii === 'clean' && p.pii_classification !== 'none' && p.pii_classification !== 'not_pii') return false;
    return true;
  });

  const piiCount = policies.filter(p => p.pii_classification !== 'none' && p.pii_classification !== 'not_pii').length;

  const content = (
    <div className="w-full overflow-hidden rounded-[28px] border border-slate-200 bg-white shadow-sm flex flex-col">
      <div className="flex justify-between items-center p-6 border-b border-slate-200">
        <div className="flex items-center gap-4">
          <div className="rounded-2xl bg-indigo-100 p-3 text-indigo-600">
            <Brain size={20} />
          </div>
          <div>
            <h2 className="text-xl font-semibold text-slate-900">LLM Reasoning & Classification</h2>
            <p className="text-sm text-slate-500">
              {policies.length} columns analyzed | {piiCount} PII detected
            </p>
          </div>
        </div>
        {!embedded && (
          <button onClick={onClose} className="rounded-full border border-slate-200 p-2 text-slate-400 transition hover:text-slate-900">
            <X size={20} />
          </button>
        )}
      </div>

      <div className="flex flex-wrap items-center gap-4 p-4 border-b border-slate-200 bg-slate-50">
        <Filter size={14} className="text-slate-400" />
        <select
          value={filterTable}
          onChange={e => setFilterTable(e.target.value)}
          className="bg-white border border-slate-200 rounded-lg px-3 py-1.5 text-sm"
        >
          <option value="all">All Tables</option>
          {tables.map(t => <option key={t} value={t}>{t}</option>)}
        </select>
        <select
          value={filterPii}
          onChange={e => setFilterPii(e.target.value)}
          className="bg-white border border-slate-200 rounded-lg px-3 py-1.5 text-sm"
        >
          <option value="all">All Classifications</option>
          <option value="pii">PII Only</option>
          <option value="clean">Non-PII Only</option>
        </select>
        <span className="text-xs text-slate-500 ml-auto">
          Showing {filtered.length} of {policies.length}
        </span>
      </div>

      <div className="overflow-auto flex-1 p-4">
        {filtered.length === 0 ? (
          <div className="text-slate-500 text-center py-12">No classification data available.</div>
        ) : (
          <div className="space-y-2">
            {filtered.map((p) => {
              const isExpanded = expandedRow === p.id;
              const piiColor = PII_COLORS[p.pii_classification] || PII_COLORS.none;
              const maskColor = MASKING_COLORS[p.masking_strategy] || 'bg-slate-100 text-slate-600 border-slate-200';

              return (
                <div key={p.id} className="rounded-2xl border border-slate-200 bg-white">
                  <button
                    className="w-full flex items-center gap-3 p-3 hover:bg-slate-50 transition-colors text-left"
                    onClick={() => setExpandedRow(isExpanded ? null : p.id)}
                  >
                    {isExpanded ? <ChevronDown size={14} className="text-slate-400 flex-shrink-0" /> : <ChevronRight size={14} className="text-slate-400 flex-shrink-0" />}
                    <span className="font-mono text-slate-900 text-sm w-32 flex-shrink-0 truncate">{p.table_name}</span>
                    <span className="font-mono text-sm w-48 truncate text-slate-700">{p.column_name}</span>
                    <span className={`px-2 py-0.5 rounded-full border text-xs ${piiColor}`}>{p.pii_classification}</span>
                    <span className={`px-2 py-0.5 rounded-full border text-xs ${maskColor}`}>{p.masking_strategy}</span>
                    {p.llm_confidence != null && (
                      <span className={`text-xs ml-auto ${p.llm_confidence >= 0.8 ? 'text-emerald-600' : p.llm_confidence >= 0.6 ? 'text-amber-600' : 'text-rose-600'}`}>
                        {(p.llm_confidence * 100).toFixed(0)}%
                      </span>
                    )}
                  </button>

                  {isExpanded && (
                    <div className="px-10 pb-4 space-y-3 border-t border-slate-200">
                      {p.sensitivity_reason && (
                        <div className="mt-3">
                          <p className="text-xs text-slate-500 mb-1 font-semibold">Reasoning</p>
                          <p className="text-sm text-slate-700 bg-slate-50 p-3 rounded-lg">{p.sensitivity_reason}</p>
                        </div>
                      )}
                      {p.notes && (
                        <div>
                          <p className="text-xs text-slate-500 mb-1 font-semibold">Notes</p>
                          <p className="text-sm text-slate-700 bg-slate-50 p-3 rounded-lg">{p.notes}</p>
                        </div>
                      )}
                      <div className="grid gap-4 md:grid-cols-3">
                        <div>
                          <p className="text-xs text-slate-500 mb-1">Business Importance</p>
                          <p className="text-sm font-medium text-slate-900">{p.business_importance || 'N/A'}</p>
                        </div>
                        <div>
                          <p className="text-xs text-slate-500 mb-1">Dedup Mode</p>
                          <p className="text-sm font-medium text-slate-900">{p.dedup_mode || 'N/A'}</p>
                        </div>
                        <div>
                          <p className="text-xs text-slate-500 mb-1">Confidence</p>
                          <p className="text-sm font-medium text-slate-900">{p.llm_confidence != null ? `${(p.llm_confidence * 100).toFixed(1)}%` : 'N/A'}</p>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );

  if (embedded) {
    return content;
  }

  return (
    <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-4">
      <div className="max-h-[90vh] w-full max-w-6xl">{content}</div>
    </div>
  );
};

export default ReasoningPanel;

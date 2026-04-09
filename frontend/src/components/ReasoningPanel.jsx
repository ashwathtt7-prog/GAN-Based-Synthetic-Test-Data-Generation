import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { X, Brain, Filter, ChevronDown, ChevronRight, Shield, Eye } from 'lucide-react';

const API_BASE = "http://localhost:8000/api";

const PII_COLORS = {
  none: 'bg-gray-700/50 text-gray-400',
  sensitive_business: 'bg-red-500/20 text-red-400',
  uncertain: 'bg-yellow-500/20 text-yellow-400',
  not_pii: 'bg-gray-700/50 text-gray-400',
};

const MASKING_COLORS = {
  passthrough: 'bg-green-500/20 text-green-400',
  suppress: 'bg-red-500/20 text-red-400',
  substitute_realistic: 'bg-blue-500/20 text-blue-400',
  format_preserving: 'bg-purple-500/20 text-purple-400',
  generalise: 'bg-yellow-500/20 text-yellow-400',
};

const ReasoningPanel = ({ onClose }) => {
  const [policies, setPolicies] = useState([]);
  const [filterTable, setFilterTable] = useState('all');
  const [filterPii, setFilterPii] = useState('all');
  const [expandedRow, setExpandedRow] = useState(null);
  const [tables, setTables] = useState([]);

  useEffect(() => {
    const fetchPolicies = async () => {
      try {
        const res = await axios.get(`${API_BASE}/policies`);
        setPolicies(res.data);
        const uniqueTables = [...new Set(res.data.map(p => p.table_name))];
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

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
      <div className="bg-gray-900 rounded-xl border border-gray-700 w-full max-w-6xl max-h-[90vh] overflow-hidden flex flex-col">
        <div className="flex justify-between items-center p-6 border-b border-gray-700">
          <div className="flex items-center gap-4">
            <Brain className="text-purple-400" size={24} />
            <div>
              <h2 className="text-xl font-bold">LLM Reasoning & Classification</h2>
              <p className="text-sm text-gray-400">
                {policies.length} columns analyzed | {piiCount} PII detected | Click a row to see reasoning
              </p>
            </div>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-white">
            <X size={24} />
          </button>
        </div>

        {/* Filters */}
        <div className="flex items-center gap-4 p-4 border-b border-gray-700/50 bg-gray-800/30">
          <Filter size={14} className="text-gray-400" />
          <select
            value={filterTable}
            onChange={e => setFilterTable(e.target.value)}
            className="bg-gray-800 border border-gray-600 rounded-lg px-3 py-1.5 text-sm"
          >
            <option value="all">All Tables</option>
            {tables.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
          <select
            value={filterPii}
            onChange={e => setFilterPii(e.target.value)}
            className="bg-gray-800 border border-gray-600 rounded-lg px-3 py-1.5 text-sm"
          >
            <option value="all">All Classifications</option>
            <option value="pii">PII Only</option>
            <option value="clean">Non-PII Only</option>
          </select>
          <span className="text-xs text-gray-500 ml-auto">
            Showing {filtered.length} of {policies.length}
          </span>
        </div>

        {/* Policy list */}
        <div className="overflow-auto flex-1 p-4">
          {filtered.length === 0 ? (
            <div className="text-gray-500 italic text-center py-12">No classification data available.</div>
          ) : (
            <div className="space-y-1">
              {filtered.map((p, i) => {
                const isExpanded = expandedRow === p.id;
                const piiColor = PII_COLORS[p.pii_classification] || PII_COLORS.none;
                const maskColor = MASKING_COLORS[p.masking_strategy] || 'bg-gray-700/50 text-gray-400';

                return (
                  <div key={p.id} className="bg-gray-800/30 rounded-lg border border-gray-700/50 overflow-hidden">
                    {/* Summary row */}
                    <button
                      className="w-full flex items-center gap-3 p-3 hover:bg-gray-800/60 transition-colors text-left"
                      onClick={() => setExpandedRow(isExpanded ? null : p.id)}
                    >
                      {isExpanded ? <ChevronDown size={14} className="text-gray-400 flex-shrink-0" /> : <ChevronRight size={14} className="text-gray-400 flex-shrink-0" />}
                      <span className="font-mono text-blue-300 text-sm w-32 flex-shrink-0 truncate">{p.table_name}</span>
                      <span className="font-mono text-sm w-48 truncate">{p.column_name}</span>
                      <span className={`px-2 py-0.5 rounded text-xs ${piiColor}`}>{p.pii_classification}</span>
                      <span className={`px-2 py-0.5 rounded text-xs ${maskColor}`}>{p.masking_strategy}</span>
                      <span className="text-xs text-gray-500 flex items-center gap-1">
                        <Shield size={10} />
                        {p.pii_source}
                      </span>
                      {p.llm_confidence != null && (
                        <span className={`text-xs ml-auto ${p.llm_confidence >= 0.8 ? 'text-green-400' : p.llm_confidence >= 0.6 ? 'text-yellow-400' : 'text-red-400'}`}>
                          {(p.llm_confidence * 100).toFixed(0)}%
                        </span>
                      )}
                    </button>

                    {/* Expanded detail */}
                    {isExpanded && (
                      <div className="px-10 pb-4 space-y-3 border-t border-gray-700/30">
                        {p.sensitivity_reason && (
                          <div className="mt-3">
                            <p className="text-xs text-gray-500 mb-1 font-semibold">Reasoning</p>
                            <p className="text-sm text-gray-300 bg-gray-800/50 p-3 rounded-lg">{p.sensitivity_reason}</p>
                          </div>
                        )}
                        {p.notes && (
                          <div>
                            <p className="text-xs text-gray-500 mb-1 font-semibold">Notes</p>
                            <p className="text-sm text-gray-300 bg-gray-800/50 p-3 rounded-lg">{p.notes}</p>
                          </div>
                        )}
                        <div className="grid grid-cols-3 gap-4">
                          <div>
                            <p className="text-xs text-gray-500 mb-1">Business Importance</p>
                            <p className="text-sm font-medium">{p.business_importance || 'N/A'}</p>
                          </div>
                          <div>
                            <p className="text-xs text-gray-500 mb-1">Dedup Mode</p>
                            <p className="text-sm font-medium">{p.dedup_mode || 'N/A'}</p>
                          </div>
                          <div>
                            <p className="text-xs text-gray-500 mb-1">Confidence</p>
                            <p className="text-sm font-medium">{p.llm_confidence != null ? `${(p.llm_confidence * 100).toFixed(1)}%` : 'N/A'}</p>
                          </div>
                        </div>
                        {p.constraint_profile && Object.keys(p.constraint_profile).length > 0 && (
                          <div>
                            <p className="text-xs text-gray-500 mb-1">Constraint Profile</p>
                            <pre className="text-xs text-gray-400 bg-gray-800/50 p-2 rounded overflow-x-auto">{JSON.stringify(p.constraint_profile, null, 2)}</pre>
                          </div>
                        )}
                        {p.edge_case_flags && p.edge_case_flags.length > 0 && (
                          <div>
                            <p className="text-xs text-gray-500 mb-1">Edge Case Flags</p>
                            <div className="flex flex-wrap gap-1">
                              {p.edge_case_flags.map((f, j) => (
                                <span key={j} className="bg-yellow-500/10 text-yellow-400 px-2 py-0.5 rounded text-xs">{f}</span>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ReasoningPanel;

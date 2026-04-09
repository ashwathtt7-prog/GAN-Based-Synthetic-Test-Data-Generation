import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { X, Cpu, Layers, BookOpen, Zap } from 'lucide-react';

const API_BASE = "http://localhost:8000/api";

const TIER_CONFIG = {
  ctgan: { label: 'CTGAN', color: 'bg-purple-500/20 text-purple-400 border-purple-500/30', icon: <Cpu size={14} /> },
  tvae:  { label: 'TVAE',  color: 'bg-blue-500/20 text-blue-400 border-blue-500/30', icon: <Layers size={14} /> },
  rule_based: { label: 'Rule-Based', color: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30', icon: <BookOpen size={14} /> },
  hybrid: { label: 'Hybrid', color: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30', icon: <Zap size={14} /> },
  unknown: { label: 'Unknown', color: 'bg-gray-500/20 text-gray-400 border-gray-500/30', icon: null },
};

const GenerationLog = ({ onClose }) => {
  const [genLog, setGenLog] = useState([]);
  const [activityLog, setActivityLog] = useState([]);
  const [tab, setTab] = useState('tiers');

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [genRes, actRes] = await Promise.all([
          axios.get(`${API_BASE}/generation/log`),
          axios.get(`${API_BASE}/pipeline/activity-log`),
        ]);
        setGenLog(genRes.data);
        setActivityLog(actRes.data);
      } catch (err) {
        console.error("Failed to fetch generation log", err);
      }
    };
    fetchData();
  }, []);

  // Extract tier routing from activity log if generation/log is empty
  const tierEntries = genLog.length > 0 ? genLog :
    activityLog
      .filter(a => a.step_name === 'tier_routing' || a.step_name === 'generation_complete')
      .map(a => ({
        table_name: a.table_name,
        tier: a.details?.tier || 'unknown',
        rows_generated: a.details?.rows_generated || a.details?.row_count || 0,
        domain: a.domain || a.details?.domain || 'unknown',
        status: a.status,
        run_id: a.run_id,
      }));

  // Deduplicate by table_name (prefer generation_complete over tier_routing)
  const tierMap = {};
  tierEntries.forEach(e => {
    if (!tierMap[e.table_name] || e.rows_generated > 0) {
      tierMap[e.table_name] = e;
    }
  });
  const dedupedEntries = Object.values(tierMap);

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-8">
      <div className="bg-gray-900 rounded-xl border border-gray-700 w-full max-w-5xl max-h-[85vh] overflow-hidden flex flex-col">
        <div className="flex justify-between items-center p-6 border-b border-gray-700">
          <div>
            <h2 className="text-xl font-bold">Generation Engine Log</h2>
            <p className="text-sm text-gray-400 mt-1">Tier routing decisions and pipeline activity</p>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-white">
            <X size={24} />
          </button>
        </div>

        {/* Tabs */}
        <div className="flex border-b border-gray-700">
          <button
            className={`px-6 py-3 text-sm font-medium transition-colors ${tab === 'tiers' ? 'text-blue-400 border-b-2 border-blue-400' : 'text-gray-400 hover:text-gray-200'}`}
            onClick={() => setTab('tiers')}
          >
            Tier Routing
          </button>
          <button
            className={`px-6 py-3 text-sm font-medium transition-colors ${tab === 'activity' ? 'text-blue-400 border-b-2 border-blue-400' : 'text-gray-400 hover:text-gray-200'}`}
            onClick={() => setTab('activity')}
          >
            Activity Log
          </button>
        </div>

        <div className="overflow-auto flex-1 p-6">
          {tab === 'tiers' ? (
            dedupedEntries.length === 0 ? (
              <div className="text-gray-500 italic text-center py-12">
                No generation data yet. Run the pipeline first.
              </div>
            ) : (
              <div className="grid gap-4">
                {dedupedEntries.map((entry, i) => {
                  const tierCfg = TIER_CONFIG[entry.tier] || TIER_CONFIG.unknown;
                  return (
                    <div key={i} className="bg-gray-800/50 rounded-lg border border-gray-700 p-4 flex items-center justify-between">
                      <div className="flex items-center gap-4">
                        <div className={`px-3 py-1.5 rounded-lg border text-sm font-semibold flex items-center gap-1.5 ${tierCfg.color}`}>
                          {tierCfg.icon}
                          {tierCfg.label}
                        </div>
                        <div>
                          <p className="font-mono font-semibold text-blue-300">{entry.table_name}</p>
                          <p className="text-xs text-gray-400 mt-0.5">Domain: {entry.domain}</p>
                        </div>
                      </div>
                      <div className="text-right">
                        <p className="text-lg font-bold">{entry.rows_generated?.toLocaleString()}</p>
                        <p className="text-xs text-gray-400">rows generated</p>
                      </div>
                    </div>
                  );
                })}

                {/* Summary */}
                <div className="mt-4 bg-gray-800/30 rounded-lg p-4 border border-gray-700/50">
                  <h3 className="text-sm font-semibold text-gray-300 mb-3">Routing Summary</h3>
                  <div className="flex gap-6">
                    {Object.entries(
                      dedupedEntries.reduce((acc, e) => {
                        acc[e.tier] = (acc[e.tier] || 0) + 1;
                        return acc;
                      }, {})
                    ).map(([tier, count]) => {
                      const cfg = TIER_CONFIG[tier] || TIER_CONFIG.unknown;
                      return (
                        <div key={tier} className="flex items-center gap-2">
                          <span className={`px-2 py-0.5 rounded text-xs border ${cfg.color}`}>{cfg.label}</span>
                          <span className="text-gray-300 font-semibold">{count} table{count !== 1 ? 's' : ''}</span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>
            )
          ) : (
            /* Activity Log Tab */
            activityLog.length === 0 ? (
              <div className="text-gray-500 italic text-center py-12">
                No activity log entries yet.
              </div>
            ) : (
              <div className="space-y-2">
                {activityLog.map((step, i) => (
                  <div key={i} className="bg-gray-800/30 rounded-lg p-3 border border-gray-700/50 flex items-start gap-3">
                    <span className={`mt-0.5 w-2 h-2 rounded-full flex-shrink-0 ${
                      step.status === 'completed' ? 'bg-green-400' :
                      step.status === 'failed' ? 'bg-red-400' :
                      step.status === 'running' ? 'bg-yellow-400' : 'bg-gray-400'
                    }`}></span>
                    <div className="flex-1 min-w-0">
                      <div className="flex justify-between items-start">
                        <span className="text-sm font-medium text-gray-200">{step.step_name}</span>
                        {step.duration_seconds && (
                          <span className="text-xs text-gray-500">{step.duration_seconds.toFixed(1)}s</span>
                        )}
                      </div>
                      {step.table_name && (
                        <span className="text-xs font-mono text-blue-300">{step.table_name}</span>
                      )}
                      {step.details && Object.keys(step.details).length > 0 && (
                        <p className="text-xs text-gray-500 mt-1 truncate">
                          {JSON.stringify(step.details).slice(0, 120)}
                        </p>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )
          )}
        </div>
      </div>
    </div>
  );
};

export default GenerationLog;

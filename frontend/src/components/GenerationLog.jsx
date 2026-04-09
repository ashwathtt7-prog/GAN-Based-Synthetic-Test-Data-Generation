import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { X, Cpu, Layers, BookOpen, Zap, Activity, TrendingUp, GaugeCircle } from 'lucide-react';

const API_BASE = "http://localhost:8001/api";

const TIER_CONFIG = {
  ctgan: { label: 'CTGAN', color: 'bg-purple-500/20 text-purple-400 border-purple-500/30', icon: <Cpu size={14} /> },
  tvae: { label: 'TVAE', color: 'bg-blue-500/20 text-blue-400 border-blue-500/30', icon: <Layers size={14} /> },
  rule_based: { label: 'Rule-Based', color: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30', icon: <BookOpen size={14} /> },
  hybrid: { label: 'Hybrid', color: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30', icon: <Zap size={14} /> },
  unknown: { label: 'Unknown', color: 'bg-gray-500/20 text-gray-400 border-gray-500/30', icon: null },
};

const STATUS_CONFIG = {
  pending: 'bg-gray-500/20 text-gray-400 border-gray-500/30',
  training: 'bg-amber-500/20 text-amber-300 border-amber-500/30',
  completed: 'bg-green-500/20 text-green-300 border-green-500/30',
  reused: 'bg-cyan-500/20 text-cyan-300 border-cyan-500/30',
};

const GenerationLog = ({ onClose }) => {
  const [genLog, setGenLog] = useState([]);
  const [activityLog, setActivityLog] = useState([]);
  const [trainingTables, setTrainingTables] = useState([]);
  const [tab, setTab] = useState('tiers');

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [genRes, actRes, trainingRes] = await Promise.all([
          axios.get(`${API_BASE}/generation/log`),
          axios.get(`${API_BASE}/pipeline/activity-log`),
          axios.get(`${API_BASE}/training-metrics`),
        ]);
        setGenLog(genRes.data);
        setActivityLog(actRes.data);
        setTrainingTables(trainingRes.data?.tables || []);
      } catch (err) {
        console.error("Failed to fetch generation insights", err);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 3000);
    return () => clearInterval(interval);
  }, []);

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

  const tierMap = {};
  tierEntries.forEach(entry => {
    if (!tierMap[entry.table_name] || entry.rows_generated > 0) {
      tierMap[entry.table_name] = entry;
    }
  });
  const dedupedEntries = Object.values(tierMap);
  const mlTrainingTables = trainingTables.filter(table =>
    table.model_type || table.model_reused || (table.metric_count || 0) > 0
  );

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-8">
      <div className="bg-gray-900 rounded-xl border border-gray-700 w-full max-w-6xl max-h-[88vh] overflow-hidden flex flex-col">
        <div className="flex justify-between items-center p-6 border-b border-gray-700">
          <div>
            <h2 className="text-xl font-bold">Generation Engine Insights</h2>
            <p className="text-sm text-gray-400 mt-1">Tier routing, training telemetry, and step-level activity</p>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-white">
            <X size={24} />
          </button>
        </div>

        <div className="flex border-b border-gray-700">
          <TabButton active={tab === 'tiers'} onClick={() => setTab('tiers')}>
            Tier Routing
          </TabButton>
          <TabButton active={tab === 'training'} onClick={() => setTab('training')}>
            Training Metrics
          </TabButton>
          <TabButton active={tab === 'activity'} onClick={() => setTab('activity')}>
            Activity Log
          </TabButton>
        </div>

        <div className="overflow-auto flex-1 p-6">
          {tab === 'tiers' && (
            dedupedEntries.length === 0 ? (
              <EmptyState message="No generation data yet. Run the pipeline first." />
            ) : (
              <div className="grid gap-4">
                {dedupedEntries.map((entry, index) => {
                  const tierCfg = TIER_CONFIG[entry.tier] || TIER_CONFIG.unknown;
                  return (
                    <div key={`${entry.run_id}-${entry.table_name}-${index}`} className="bg-gray-800/50 rounded-lg border border-gray-700 p-4 flex items-center justify-between">
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
                        <p className="text-lg font-bold">{entry.rows_generated?.toLocaleString?.() || entry.rows_generated}</p>
                        <p className="text-xs text-gray-400">rows generated</p>
                      </div>
                    </div>
                  );
                })}

                <div className="mt-4 bg-gray-800/30 rounded-lg p-4 border border-gray-700/50">
                  <h3 className="text-sm font-semibold text-gray-300 mb-3">Routing Summary</h3>
                  <div className="flex gap-6 flex-wrap">
                    {Object.entries(
                      dedupedEntries.reduce((acc, entry) => {
                        acc[entry.tier] = (acc[entry.tier] || 0) + 1;
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
          )}

          {tab === 'training' && (
            mlTrainingTables.length === 0 ? (
              <EmptyState message="No ML training metrics yet. CTGAN or TVAE tables will appear here during a run." />
            ) : (
              <div className="grid gap-4">
                {mlTrainingTables.map(table => (
                  <TrainingCard key={table.table_name} table={table} />
                ))}
              </div>
            )
          )}

          {tab === 'activity' && (
            activityLog.length === 0 ? (
              <EmptyState message="No activity log entries yet." />
            ) : (
              <div className="space-y-2">
                {activityLog.map((step, index) => (
                  <div key={`${step.id || index}`} className="bg-gray-800/30 rounded-lg p-3 border border-gray-700/50 flex items-start gap-3">
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
                          {JSON.stringify(step.details).slice(0, 140)}
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

const TabButton = ({ active, onClick, children }) => (
  <button
    className={`px-6 py-3 text-sm font-medium transition-colors ${active ? 'text-blue-400 border-b-2 border-blue-400' : 'text-gray-400 hover:text-gray-200'}`}
    onClick={onClick}
  >
    {children}
  </button>
);

const EmptyState = ({ message }) => (
  <div className="text-gray-500 italic text-center py-12">
    {message}
  </div>
);

const TrainingCard = ({ table }) => {
  const tierCfg = TIER_CONFIG[table.tier] || TIER_CONFIG[table.model_type] || TIER_CONFIG.unknown;
  const statusClass = STATUS_CONFIG[table.status] || STATUS_CONFIG.pending;
  const latestMetric = table.latest_metric || {};
  const primarySeries = (table.metrics || []).map(metric =>
    metric.model_type === 'ctgan' ? metric.generator_loss : metric.loss
  );

  return (
    <div className="bg-gray-800/50 rounded-xl border border-gray-700 p-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="flex items-center gap-3 flex-wrap">
            <p className="font-mono font-semibold text-blue-300">{table.table_name}</p>
            <span className={`px-2.5 py-1 rounded-full text-xs border ${tierCfg.color}`}>{tierCfg.label}</span>
            <span className={`px-2.5 py-1 rounded-full text-xs border ${statusClass}`}>
              {table.model_reused ? 'Model Reused' : (table.status || 'pending')}
            </span>
          </div>
          <p className="text-xs text-gray-400 mt-2">
            Domain: {table.domain} · Modeled: {table.profile?.modeled_columns || 0} · Structural: {table.profile?.structural_columns || 0}
          </p>
        </div>
        <div className="text-right text-sm">
          <p className="text-gray-300 font-semibold">{table.model_type?.toUpperCase?.() || 'N/A'}</p>
          <p className="text-xs text-gray-500">
            Epochs: {table.epochs_completed || 0}{table.epochs_planned ? ` / ${table.epochs_planned}` : ''}
          </p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mt-4">
        <MetricBox
          icon={<TrendingUp size={16} />}
          label={table.model_type === 'ctgan' ? 'Generator Loss' : 'Loss'}
          value={formatMetric(table.model_type === 'ctgan' ? latestMetric.generator_loss : latestMetric.loss)}
        />
        <MetricBox
          icon={<GaugeCircle size={16} />}
          label="Discriminator Loss"
          value={table.model_type === 'ctgan' ? formatMetric(latestMetric.discriminator_loss) : 'N/A'}
        />
        <MetricBox
          icon={<Activity size={16} />}
          label="Metric Points"
          value={(table.metric_count || 0).toString()}
        />
      </div>

      <div className="mt-4 bg-gray-900/60 rounded-lg border border-gray-700/60 p-4">
        <div className="flex justify-between items-center mb-3">
          <p className="text-sm font-semibold text-gray-300">Training Curve</p>
          {table.model_path && (
            <p className="text-[11px] text-gray-500 truncate max-w-[360px]">{table.model_path}</p>
          )}
        </div>
        <Sparkline values={primarySeries} />
      </div>
    </div>
  );
};

const MetricBox = ({ icon, label, value }) => (
  <div className="bg-gray-900/50 rounded-lg border border-gray-700/60 p-3">
    <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
      {icon}
      {label}
    </div>
    <div className="text-lg font-semibold text-white">{value}</div>
  </div>
);

const Sparkline = ({ values }) => {
  if (!values || values.length < 2) {
    return (
      <div className="h-24 rounded-md border border-dashed border-gray-700 flex items-center justify-center text-sm text-gray-500">
        Waiting for more training points...
      </div>
    );
  }

  const width = 320;
  const height = 88;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const points = values.map((value, index) => {
    const x = (index / (values.length - 1)) * width;
    const y = height - ((value - min) / range) * (height - 8) - 4;
    return `${x},${y}`;
  }).join(' ');

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="w-full h-24">
      <polyline
        fill="none"
        stroke="#60a5fa"
        strokeWidth="3"
        strokeLinejoin="round"
        strokeLinecap="round"
        points={points}
      />
    </svg>
  );
};

const formatMetric = (value) => {
  if (value === undefined || value === null || Number.isNaN(Number(value))) {
    return 'N/A';
  }
  return Number(value).toFixed(4);
};

export default GenerationLog;

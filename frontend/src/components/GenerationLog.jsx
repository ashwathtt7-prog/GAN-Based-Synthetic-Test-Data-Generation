import React, { useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import { Activity, BookOpen, Cpu, GaugeCircle, Layers, TrendingUp, X, Zap } from 'lucide-react';

const API_BASE = "http://localhost:8001/api";

const TIER_CONFIG = {
  ctgan: { label: 'CTGAN', color: 'bg-fuchsia-100 text-fuchsia-700 border-fuchsia-200', icon: <Cpu size={14} /> },
  tvae: { label: 'TVAE', color: 'bg-sky-100 text-sky-700 border-sky-200', icon: <Layers size={14} /> },
  rule_based: { label: 'Rule-Based', color: 'bg-emerald-100 text-emerald-700 border-emerald-200', icon: <BookOpen size={14} /> },
  hybrid: { label: 'Hybrid', color: 'bg-amber-100 text-amber-700 border-amber-200', icon: <Zap size={14} /> },
  pending: { label: 'Pending', color: 'bg-slate-100 text-slate-700 border-slate-200', icon: <Activity size={14} /> },
  unknown: { label: 'Unknown', color: 'bg-slate-100 text-slate-700 border-slate-200', icon: <Activity size={14} /> },
};

const STATUS_CONFIG = {
  pending: 'bg-slate-100 text-slate-700 border-slate-200',
  running: 'bg-blue-100 text-blue-700 border-blue-200',
  training: 'bg-amber-100 text-amber-700 border-amber-200',
  completed: 'bg-emerald-100 text-emerald-700 border-emerald-200',
  reused: 'bg-cyan-100 text-cyan-700 border-cyan-200',
  failed: 'bg-rose-100 text-rose-700 border-rose-200',
  waiting_review: 'bg-orange-100 text-orange-700 border-orange-200',
};

const TabButton = ({ active, onClick, children }) => (
  <button
    className={`rounded-full px-4 py-2 text-sm font-medium transition-colors ${
      active ? 'bg-slate-900 text-white' : 'text-slate-500 hover:bg-slate-100 hover:text-slate-900'
    }`}
    onClick={onClick}
  >
    {children}
  </button>
);

const EmptyState = ({ message }) => (
  <div className="rounded-3xl border border-dashed border-slate-200 bg-white px-6 py-16 text-center text-sm text-slate-500">
    {message}
  </div>
);

const MetricBox = ({ icon, label, value }) => (
  <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
    <div className="mb-2 flex items-center gap-2 text-xs text-slate-500">
      {icon}
      {label}
    </div>
    <div className="text-lg font-semibold text-slate-900">{value}</div>
  </div>
);

const Sparkline = ({ values }) => {
  if (!values || values.length < 2) {
    return (
      <div className="flex h-24 items-center justify-center rounded-2xl border border-dashed border-slate-200 text-sm text-slate-500">
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
    <svg viewBox={`0 0 ${width} ${height}`} className="h-24 w-full">
      <polyline
        fill="none"
        stroke="#38bdf8"
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

const TrainingCard = ({ table }) => {
  const tierCfg = TIER_CONFIG[table.tier] || TIER_CONFIG[table.model_type] || TIER_CONFIG.unknown;
  const statusClass = STATUS_CONFIG[table.status] || STATUS_CONFIG.pending;
  const latestMetric = table.latest_metric || {};
  const primarySeries = (table.metrics || []).map((metric) =>
    metric.model_type === 'ctgan' ? metric.generator_loss : metric.loss
  );

  return (
    <div className="rounded-3xl border border-slate-200 bg-white p-5 shadow-sm">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <div className="flex flex-wrap items-center gap-3">
            <p className="font-mono text-sm font-semibold text-slate-900">{table.table_name}</p>
            <span className={`rounded-full border px-2.5 py-1 text-xs ${tierCfg.color}`}>{tierCfg.label}</span>
            <span className={`rounded-full border px-2.5 py-1 text-xs ${statusClass}`}>
              {table.model_reused ? 'Model Reused' : (table.status || 'pending')}
            </span>
          </div>
          <p className="mt-2 text-xs text-slate-500">
            Domain: {table.domain} | Modeled: {table.profile?.modeled_columns || 0} | Structural: {table.profile?.structural_columns || 0}
          </p>
        </div>
        <div className="text-right text-sm">
          <p className="font-semibold text-slate-900">{table.model_type?.toUpperCase?.() || 'N/A'}</p>
          <p className="text-xs text-slate-500">
            Epochs: {table.epochs_completed || 0}{table.epochs_planned ? ` / ${table.epochs_planned}` : ''}
          </p>
        </div>
      </div>

      <div className="mt-4 grid gap-4 lg:grid-cols-3">
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
          value={`${table.metric_count || 0}`}
        />
      </div>

      <div className="mt-4 rounded-2xl border border-slate-200 bg-slate-50 p-4">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <p className="text-sm font-semibold text-slate-700">Training Curve</p>
          {table.model_path && <p className="max-w-[360px] truncate text-[11px] text-slate-500">{table.model_path}</p>}
        </div>
        <Sparkline values={primarySeries} />
      </div>
    </div>
  );
};

const GenerationLog = ({ onClose, runId = null, embedded = false }) => {
  const [genLog, setGenLog] = useState([]);
  const [activityLog, setActivityLog] = useState([]);
  const [trainingTables, setTrainingTables] = useState([]);
  const [tab, setTab] = useState('tiers');

  useEffect(() => {
    const fetchData = async () => {
      try {
        const params = runId ? { run_id: runId } : {};
        const [genRes, actRes, trainingRes] = await Promise.all([
          axios.get(`${API_BASE}/generation/log`, { params }),
          axios.get(`${API_BASE}/pipeline/activity-log`, { params }),
          axios.get(`${API_BASE}/training-metrics`, { params }),
        ]);
        setGenLog(genRes.data || []);
        setActivityLog(actRes.data || []);
        setTrainingTables(trainingRes.data?.tables || []);
      } catch (err) {
        console.error("Failed to fetch generation insights", err);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 3000);
    return () => clearInterval(interval);
  }, [runId]);

  const dedupedEntries = useMemo(() => {
    const tierEntries = genLog.length > 0
      ? genLog
      : activityLog
          .filter((a) => a.step_name === 'tier_routing' || a.step_name === 'generation_complete')
          .map((a) => ({
            table_name: a.table_name,
            tier: a.details?.tier || 'unknown',
            rows_generated: a.details?.rows_generated || a.details?.row_count || 0,
            domain: a.domain || a.details?.domain || 'unknown',
            status: a.status,
            run_id: a.run_id,
          }));

    const tierMap = {};
    tierEntries.forEach((entry) => {
      const existing = tierMap[entry.table_name];
      if (!existing || (existing.rows_generated || 0) <= (entry.rows_generated || 0)) {
        tierMap[entry.table_name] = entry;
      }
    });

    return Object.values(tierMap);
  }, [activityLog, genLog]);

  const mlTrainingTables = trainingTables.filter((table) =>
    table.model_type || table.model_reused || (table.metric_count || 0) > 0
  );

  const content = (
    <div className="flex h-full flex-col overflow-hidden rounded-[28px] border border-slate-200 bg-white shadow-sm">
      <div className="flex flex-wrap items-start justify-between gap-4 border-b border-slate-200 px-6 py-5">
        <div>
          <h2 className="text-xl font-semibold text-slate-900">Generation Engine Insights</h2>
          <p className="mt-1 text-sm text-slate-500">
            Tier routing, training telemetry, and activity for {runId ? `run ${runId.slice(0, 8)}` : 'the latest run'}.
          </p>
        </div>
        {!embedded && (
          <button onClick={onClose} className="rounded-full border border-slate-200 p-2 text-slate-400 transition hover:text-slate-900">
            <X size={20} />
          </button>
        )}
      </div>

      <div className="flex flex-wrap gap-2 border-b border-slate-200 px-6 py-4">
        <TabButton active={tab === 'tiers'} onClick={() => setTab('tiers')}>Tier Routing</TabButton>
        <TabButton active={tab === 'training'} onClick={() => setTab('training')}>Training Metrics</TabButton>
        <TabButton active={tab === 'activity'} onClick={() => setTab('activity')}>Activity Log</TabButton>
      </div>

      <div className="flex-1 overflow-auto p-6">
        {tab === 'tiers' && (
          dedupedEntries.length === 0 ? (
            <EmptyState message="No generation data yet. Start a run to see table routing." />
          ) : (
            <div className="grid gap-4">
              {dedupedEntries.map((entry) => {
                const tierCfg = TIER_CONFIG[entry.tier] || TIER_CONFIG.unknown;
                const statusClass = STATUS_CONFIG[entry.status] || STATUS_CONFIG.pending;
                return (
                  <div key={`${entry.run_id}-${entry.table_name}`} className="flex flex-wrap items-center justify-between gap-4 rounded-3xl border border-slate-200 bg-white p-5 shadow-sm">
                    <div className="min-w-0">
                      <div className="flex flex-wrap items-center gap-3">
                        <div className={`flex items-center gap-1.5 rounded-full border px-3 py-1.5 text-sm font-semibold ${tierCfg.color}`}>
                          {tierCfg.icon}
                          {tierCfg.label}
                        </div>
                        <div className={`rounded-full border px-3 py-1 text-xs ${statusClass}`}>
                          {entry.status || 'pending'}
                        </div>
                      </div>
                      <p className="mt-3 font-mono text-base font-semibold text-slate-900">{entry.table_name}</p>
                      <p className="mt-1 text-sm text-slate-500">Domain: {entry.domain || 'unknown'}</p>
                    </div>
                    <div className="text-right">
                      <p className="text-3xl font-semibold text-slate-900">
                        {entry.rows_generated ? entry.rows_generated.toLocaleString() : '--'}
                      </p>
                      <p className="text-xs uppercase tracking-[0.2em] text-slate-500">
                        {entry.rows_generated ? 'rows generated' : 'not generated yet'}
                      </p>
                    </div>
                  </div>
                );
              })}
            </div>
          )
        )}

        {tab === 'training' && (
          mlTrainingTables.length === 0 ? (
            <EmptyState message="No ML training metrics yet. CTGAN or TVAE tables will appear here during a run." />
          ) : (
            <div className="grid gap-4">
              {mlTrainingTables.map((table) => (
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
                <div key={`${step.id || index}`} className="flex items-start gap-3 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
                  <span className={`mt-1 h-2.5 w-2.5 rounded-full ${
                    step.status === 'completed' ? 'bg-emerald-500' :
                    step.status === 'failed' ? 'bg-rose-500' :
                    step.status === 'running' ? 'bg-sky-500' :
                    step.status === 'waiting_review' ? 'bg-orange-500' : 'bg-slate-400'
                  }`}></span>
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-start justify-between gap-2">
                      <span className="text-sm font-medium text-slate-900">{step.step_name}</span>
                      {step.duration_seconds && <span className="text-xs text-slate-500">{step.duration_seconds.toFixed(1)}s</span>}
                    </div>
                    {step.table_name && <p className="mt-1 font-mono text-xs text-slate-700">{step.table_name}</p>}
                    {step.details && Object.keys(step.details).length > 0 && (
                      <p className="mt-2 truncate text-xs text-slate-500">{JSON.stringify(step.details)}</p>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )
        )}
      </div>
    </div>
  );

  if (embedded) {
    return content;
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-8">
      <div className="h-[88vh] w-full max-w-6xl">{content}</div>
    </div>
  );
};

export default GenerationLog;

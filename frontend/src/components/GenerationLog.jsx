import React, { useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import {
  Bot,
  Clock3,
  LoaderCircle,
  ScrollText,
  Sparkles,
  X,
} from 'lucide-react';

const API_BASE = "http://localhost:8001/api";

const STATUS_STYLES = {
  pending: {
    badge: 'bg-slate-100 text-slate-600 border-slate-200',
    dot: 'bg-slate-300',
  },
  running: {
    badge: 'bg-sky-100 text-sky-700 border-sky-200',
    dot: 'bg-sky-500',
  },
  waiting_review: {
    badge: 'bg-amber-100 text-amber-700 border-amber-200',
    dot: 'bg-amber-500',
  },
  completed: {
    badge: 'bg-emerald-100 text-emerald-700 border-emerald-200',
    dot: 'bg-emerald-500',
  },
  failed: {
    badge: 'bg-rose-100 text-rose-700 border-rose-200',
    dot: 'bg-rose-500',
  },
};

const PHASE_ACCENTS = [
  'from-sky-500/25 via-sky-500/5 to-transparent',
  'from-violet-500/20 via-violet-500/5 to-transparent',
  'from-cyan-500/20 via-cyan-500/5 to-transparent',
  'from-emerald-500/20 via-emerald-500/5 to-transparent',
  'from-amber-500/20 via-amber-500/5 to-transparent',
];

const formatDuration = (seconds) => {
  if (!seconds || Number.isNaN(Number(seconds))) return '0s';
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  if (mins === 0) return `${secs}s`;
  return `${mins}m ${secs}s`;
};

const formatStamp = (value) => {
  if (!value) return 'Live';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return 'Live';
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
};

const formatValue = (value) => {
  if (Array.isArray(value)) {
    return value.length === 0 ? '[]' : value.join(', ');
  }
  if (value && typeof value === 'object') {
    return JSON.stringify(value);
  }
  if (value === null || value === undefined || value === '') {
    return 'n/a';
  }
  return String(value);
};

const StatusBadge = ({ status }) => {
  const style = STATUS_STYLES[status] || STATUS_STYLES.pending;
  return (
    <span className={`rounded-full border px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] ${style.badge}`}>
      {status === 'waiting_review' ? 'Waiting Review' : status}
    </span>
  );
};

const PhaseCard = ({ phase, index, isActive, isLast }) => {
  const style = STATUS_STYLES[phase.status] || STATUS_STYLES.pending;
  const accent = PHASE_ACCENTS[index % PHASE_ACCENTS.length];

  return (
    <div className="relative pl-10">
      {!isLast && <div className="absolute left-[17px] top-10 h-[calc(100%-1rem)] w-px bg-slate-200" />}
      <div className={`absolute left-0 top-2 flex h-9 w-9 items-center justify-center rounded-full border border-white bg-white shadow-sm ${isActive ? 'ring-4 ring-sky-100' : ''}`}>
        <span className={`h-3.5 w-3.5 rounded-full ${style.dot}`}></span>
      </div>

      <div className="overflow-hidden rounded-[28px] border border-slate-200 bg-white shadow-sm">
        <div className={`bg-gradient-to-r ${accent} px-5 py-4`}>
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.24em] text-slate-400">Phase {index + 1}</p>
              <h3 className="mt-1 text-lg font-semibold text-slate-900">{phase.label}</h3>
              <p className="mt-1 text-sm text-slate-500">{phase.description}</p>
            </div>
            <StatusBadge status={phase.status} />
          </div>

          <div className="mt-4 h-2 overflow-hidden rounded-full bg-white/80">
            <div
              className="h-full rounded-full bg-slate-900 transition-all duration-500"
              style={{ width: `${Math.max(0, Math.min(100, phase.progress_pct || 0))}%` }}
            ></div>
          </div>

          <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-xs text-slate-500">
            <span>{Math.round(phase.progress_pct || 0)}% within phase</span>
            <span>{phase.log_count || 0} events</span>
          </div>
        </div>

        <div className="space-y-4 px-5 py-5">
          <div>
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">Latest update</p>
            <p className="mt-2 text-sm text-slate-700">
              {phase.latest_message || 'Waiting for this phase to begin.'}
            </p>
          </div>

          <div className="rounded-2xl border border-sky-100 bg-sky-50/80 p-4">
            <div className="flex items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-sky-700">
              <Bot size={14} />
              LLM insight
            </div>
            <p className="mt-2 text-sm leading-6 text-slate-700">
              {phase.llm_insight || 'No LLM-specific reasoning surfaced for this phase yet; the rule engine will continue logging deterministic work as it happens.'}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

const LogEntry = ({ entry }) => {
  const style = STATUS_STYLES[entry.status] || STATUS_STYLES.pending;
  const detailEntries = Object.entries(entry.details || {}).filter(
    ([key]) => !['message', 'llm_insight', 'phase_id'].includes(key)
  );

  return (
    <div className="rounded-[24px] border border-slate-200 bg-white p-4 shadow-sm">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-3">
            <span className={`h-2.5 w-2.5 rounded-full ${style.dot}`}></span>
            <p className="text-sm font-semibold text-slate-900">{entry.title}</p>
            {entry.phase_id && (
              <span className="rounded-full bg-slate-100 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">
                {entry.phase_id.replace('_', ' ')}
              </span>
            )}
          </div>

          <div className="mt-2 flex flex-wrap gap-2 text-[11px] uppercase tracking-[0.14em] text-slate-400">
            {entry.table_name && <span>{entry.table_name}</span>}
            {entry.domain && <span>{entry.domain}</span>}
            <span>{formatStamp(entry.completed_at || entry.started_at)}</span>
          </div>
        </div>

        <StatusBadge status={entry.status} />
      </div>

      <p className="mt-4 text-sm leading-6 text-slate-700">{entry.message}</p>

      {entry.llm_insight && (
        <div className="mt-4 rounded-2xl border border-violet-100 bg-violet-50/80 p-4">
          <div className="flex items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-violet-700">
            <Sparkles size={14} />
            LLM insight
          </div>
          <p className="mt-2 text-sm leading-6 text-slate-700">{entry.llm_insight}</p>
        </div>
      )}

      {detailEntries.length > 0 && (
        <div className="mt-4 grid gap-2 md:grid-cols-2">
          {detailEntries.map(([key, value]) => (
            <div key={key} className="rounded-2xl bg-slate-50 px-3 py-2">
              <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-400">{key.replace(/_/g, ' ')}</p>
              <p className="mt-1 break-words font-mono text-xs text-slate-600">{formatValue(value)}</p>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

const EmptyState = ({ title, message }) => (
  <div className="rounded-[28px] border border-dashed border-slate-200 bg-white px-6 py-16 text-center">
    <p className="text-lg font-semibold text-slate-900">{title}</p>
    <p className="mt-2 text-sm text-slate-500">{message}</p>
  </div>
);

const GenerationLog = ({ onClose, runId = null, embedded = false }) => {
  const [progress, setProgress] = useState(null);
  const [error, setError] = useState('');

  useEffect(() => {
    let cancelled = false;

    const fetchProgress = async () => {
      try {
        const params = runId ? { run_id: runId } : {};
        const res = await axios.get(`${API_BASE}/generation/progress`, { params });
        if (!cancelled) {
          setProgress(res.data);
          setError('');
        }
      } catch {
        if (!cancelled) {
          setError('Unable to load live generation progress.');
        }
      }
    };

    fetchProgress();
    const interval = setInterval(fetchProgress, 1500);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [runId]);

  const logs = useMemo(() => {
    const entries = progress?.logs || [];
    return [...entries].reverse();
  }, [progress]);

  const currentPhase = progress?.phases?.find((phase) => phase.id === progress?.current_phase_id);

  const content = (
    <div className="flex h-full flex-col overflow-hidden rounded-[30px] border border-slate-200 bg-white shadow-sm">
      <div className="border-b border-slate-200 px-6 py-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.28em] text-slate-400">Live generation</p>
            <h2 className="mt-2 text-2xl font-semibold text-slate-900">Rule-Based Generation Progress</h2>
            <p className="mt-2 max-w-3xl text-sm text-slate-500">
              One live screen for deterministic synthesis, phase-by-phase progress, LLM reasoning, and raw execution logs.
            </p>
          </div>
          {!embedded && (
            <button onClick={onClose} className="rounded-full border border-slate-200 p-2 text-slate-400 transition hover:text-slate-900">
              <X size={20} />
            </button>
          )}
        </div>

        <div className="mt-5 grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(320px,1fr)]">
          <div className="rounded-[28px] border border-slate-200 bg-slate-50/80 p-5">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">Overall progress</p>
                <p className="mt-2 text-lg font-semibold text-slate-900">{progress?.current_step || 'Waiting for a run'}</p>
              </div>
              <StatusBadge status={progress?.status || 'pending'} />
            </div>

            <div className="mt-4 h-3 overflow-hidden rounded-full bg-white">
              <div
                className="h-full rounded-full bg-slate-900 transition-all duration-500"
                style={{ width: `${Math.max(0, Math.min(100, progress?.progress_pct || 0))}%` }}
              ></div>
            </div>

            <div className="mt-3 flex flex-wrap items-center justify-between gap-3 text-sm text-slate-500">
              <span>{Math.round(progress?.progress_pct || 0)}% complete</span>
              <span className="flex items-center gap-2">
                <Clock3 size={14} />
                {formatDuration(progress?.elapsed_seconds || 0)}
              </span>
            </div>
          </div>

          <div className="rounded-[28px] border border-slate-200 bg-slate-900 p-5 text-white shadow-sm">
            <div className="flex items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-300">
              <LoaderCircle size={14} />
              Current phase
            </div>
            <p className="mt-3 text-xl font-semibold">{currentPhase?.label || 'Awaiting run'}</p>
            <p className="mt-2 text-sm leading-6 text-slate-300">
              {currentPhase?.latest_message || 'Start a pipeline run to stream phase updates here.'}
            </p>
            <div className="mt-4 rounded-2xl bg-white/8 p-4">
              <div className="flex items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-sky-200">
                <Bot size={14} />
                Latest LLM insight
              </div>
              <p className="mt-2 text-sm leading-6 text-slate-100">
                {currentPhase?.llm_insight || 'LLM-backed reasoning will appear here as the pipeline classifies fields and refines rule plans.'}
              </p>
            </div>
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-auto bg-slate-50/70 p-6">
        {error ? (
          <EmptyState title="Progress unavailable" message={error} />
        ) : !progress?.run_id ? (
          <EmptyState
            title="No generation run selected"
            message="Start a new pipeline run from the overview screen and this panel will begin streaming phase updates automatically."
          />
        ) : (
          <div className="grid gap-6 xl:grid-cols-[minmax(0,1.1fr)_minmax(0,1fr)]">
            <div className="space-y-4">
              {progress.phases.map((phase, index) => (
                <PhaseCard
                  key={phase.id}
                  phase={phase}
                  index={index}
                  isActive={phase.id === progress.current_phase_id}
                  isLast={index === progress.phases.length - 1}
                />
              ))}
            </div>

            <div className="space-y-4">
              <div className="rounded-[28px] border border-slate-200 bg-white p-5 shadow-sm">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <div className="flex items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">
                      <ScrollText size={14} />
                      Live logs
                    </div>
                    <p className="mt-2 text-lg font-semibold text-slate-900">Execution stream</p>
                  </div>
                  <div className="rounded-full bg-slate-100 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">
                    Latest first
                  </div>
                </div>
                <p className="mt-2 text-sm text-slate-500">
                  Every emitted backend event is listed here, including messages, table context, and surfaced reasoning.
                </p>
              </div>

              <div className="space-y-3">
                {logs.length === 0 ? (
                  <EmptyState
                    title="No logs yet"
                    message="The run has started, but no generation events have been emitted yet."
                  />
                ) : (
                  logs.map((entry) => <LogEntry key={entry.id} entry={entry} />)
                )}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );

  if (embedded) {
    return content;
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-8">
      <div className="h-[90vh] w-full max-w-7xl">{content}</div>
    </div>
  );
};

export default GenerationLog;

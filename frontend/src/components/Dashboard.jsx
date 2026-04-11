import React, { useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import {
  Activity,
  Brain,
  CheckCircle,
  Database,
  LayoutGrid,
  Network,
  PlayCircle,
  ShieldCheck,
  Sparkles,
  Timer,
} from 'lucide-react';
import GenerationLog from './GenerationLog';
import DataViewer from './DataViewer';
import GraphView from './GraphView';
import ReasoningPanel from './ReasoningPanel';

const API_BASE = "http://localhost:8001/api";

const MENU_ITEMS = [
  { id: 'overview', label: 'Overview', icon: LayoutGrid },
  { id: 'review', label: 'Human Review', icon: ShieldCheck },
  { id: 'insights', label: 'Live Progress', icon: Sparkles },
  { id: 'data', label: 'View Data', icon: Database },
  { id: 'graph', label: 'Graph', icon: Network },
  { id: 'policies', label: 'Policies', icon: Brain },
];

const FALLBACK_SOURCES = [
  {
    name: 'telecom_poc',
    label: 'Telecom Source DB',
    description: '22-table telecom source with customer, billing, and network domains.',
    table_count: 22,
    is_default: true,
  },
  {
    name: 'demo_showcase',
    label: 'Retail Mini Demo DB',
    description: '4-table relational demo with customers, products, orders, and order items.',
    table_count: 4,
    is_default: false,
  },
];

const STATUS_BADGE = {
  running: 'bg-sky-100 text-sky-700 border-sky-200',
  completed: 'bg-emerald-100 text-emerald-700 border-emerald-200',
  failed: 'bg-rose-100 text-rose-700 border-rose-200',
  waiting_review: 'bg-amber-100 text-amber-700 border-amber-200',
  initialized: 'bg-slate-100 text-slate-600 border-slate-200',
  idle: 'bg-slate-100 text-slate-600 border-slate-200',
};

const formatDuration = (seconds) => {
  if (!seconds || Number.isNaN(Number(seconds))) return '0s';
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  if (mins === 0) return `${secs}s`;
  return `${mins}m ${secs}s`;
};

const parseTableFilter = (raw) => {
  const items = (raw || '')
    .split(/[\n,]+/)
    .map((item) => item.trim())
    .filter(Boolean);
  return items.length > 0 ? items : null;
};

const Dashboard = () => {
  const [active, setActive] = useState('overview');
  const [runs, setRuns] = useState([]);
  const [dataSources, setDataSources] = useState(FALLBACK_SOURCES);
  const [loadingSources, setLoadingSources] = useState(true);
  const [selectedSourceName, setSelectedSourceName] = useState('telecom_poc');
  const [selectedRunId, setSelectedRunId] = useState(null);
  const [status, setStatus] = useState(null);
  const [stats, setStats] = useState(null);
  const [blockingReviews, setBlockingReviews] = useState([]);
  const [tableFilterText, setTableFilterText] = useState('');
  const [starting, setStarting] = useState(false);
  const [startMessage, setStartMessage] = useState('');
  const [startError, setStartError] = useState('');
  const [reviewNotes, setReviewNotes] = useState({});

  const activeRun = useMemo(
    () => runs.find((run) => run.run_id === selectedRunId) || null,
    [runs, selectedRunId]
  );

  const selectedSource = useMemo(
    () =>
      dataSources.find((source) => source.name === selectedSourceName) ||
      FALLBACK_SOURCES.find((source) => source.name === selectedSourceName) ||
      null,
    [dataSources, selectedSourceName]
  );

  const availableSources = useMemo(
    () => (dataSources.length > 0 ? dataSources : FALLBACK_SOURCES),
    [dataSources]
  );

  useEffect(() => {
    const fetchSources = async () => {
      try {
        setLoadingSources(true);
        const res = await axios.get(`${API_BASE}/data-sources`);
        const list = res.data || [];
        if (list.length > 0) {
          setDataSources(list);
          setSelectedSourceName((current) => {
            const stillValid = list.some((source) => source.name === current);
            if (stillValid) {
              return current;
            }
            return list.find((source) => source.is_default)?.name || list[0]?.name || '';
          });
          return;
        }
      } catch (err) {
        console.error("Failed to fetch data sources", err);
      } finally {
        setLoadingSources(false);
      }

      setDataSources((current) => (current.length > 0 ? current : FALLBACK_SOURCES));
      setSelectedSourceName((current) => current || FALLBACK_SOURCES[0].name);
    };

    fetchSources();
    const interval = setInterval(fetchSources, 10000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    const fetchRuns = async () => {
      try {
        const res = await axios.get(`${API_BASE}/pipeline/runs`);
        const list = res.data || [];
        setRuns(list);
        if (!selectedRunId && list.length > 0) {
          setSelectedRunId(list[0].run_id);
        }
        if (selectedRunId && list.length > 0 && !list.some((r) => r.run_id === selectedRunId)) {
          setSelectedRunId(list[0].run_id);
        }
      } catch (err) {
        console.error("Failed to fetch runs", err);
      }
    };

    fetchRuns();
    const interval = setInterval(fetchRuns, 5000);
    return () => clearInterval(interval);
  }, [selectedRunId]);

  useEffect(() => {
    const refresh = async () => {
      try {
        if (selectedRunId) {
          const [statusRes, statsRes, reviewRes] = await Promise.all([
            axios.get(`${API_BASE}/pipeline/status/${selectedRunId}`),
            axios.get(`${API_BASE}/dashboard/stats`, { params: { run_id: selectedRunId } }),
            axios.get(`${API_BASE}/review/queue`, { params: { run_id: selectedRunId, blocking_only: true } }),
          ]);
          setStatus(statusRes.data);
          setStats(statsRes.data);
          setBlockingReviews(reviewRes.data || []);
        } else {
          const statsRes = await axios.get(`${API_BASE}/dashboard/stats`);
          setStats(statsRes.data);
          setStatus(null);
          setBlockingReviews([]);
        }
      } catch (err) {
        console.error("Failed to refresh dashboard", err);
      }
    };

    refresh();
    const interval = setInterval(refresh, 3000);
    return () => clearInterval(interval);
  }, [selectedRunId]);

  const startPipeline = async () => {
    setStarting(true);
    setStartError('');
    setStartMessage('');
    try {
      const tableFilter = parseTableFilter(tableFilterText);
      const res = await axios.post(`${API_BASE}/pipeline/start`, {
        table_filter: tableFilter,
        fast_mode: true,
        source_name: selectedSourceName || undefined,
      });
      setSelectedRunId(res.data?.run_id || null);
      setStartMessage(`Run started: ${res.data?.run_id?.slice(0, 8) || 'new run'} on ${res.data?.source_name || selectedSourceName || 'default source'}`);
      setActive('insights');
    } catch {
      setStartError('Failed to start pipeline. Check the backend logs.');
    } finally {
      setStarting(false);
    }
  };

  const approveReview = async (itemId) => {
    try {
      await axios.post(`${API_BASE}/review/${itemId}/approve`, {
        reviewer_notes: reviewNotes[itemId] || '',
      });
      setReviewNotes((prev) => ({ ...prev, [itemId]: '' }));
    } catch (err) {
      console.error("Failed to approve review item", err);
    }
  };

  return (
    <div className="flex min-h-screen flex-col md:flex-row">
      <aside className="w-full md:w-64 border-b md:border-b-0 md:border-r border-slate-200 bg-white/80 backdrop-blur">
        <div className="p-6">
          <p className="text-xs uppercase tracking-[0.3em] text-slate-400">Synthetic Data</p>
          <h1 className="mt-2 text-xl font-semibold text-slate-900">Rule-Based Control Room</h1>
          <p className="mt-1 text-sm text-slate-500">Live orchestration console</p>
        </div>

        <nav className="flex gap-2 px-4 pb-4 md:flex-col md:gap-1 md:px-4 md:pb-6 overflow-x-auto">
          {MENU_ITEMS.map((item) => {
            const Icon = item.icon;
            const isActive = active === item.id;
            return (
              <button
                key={item.id}
                onClick={() => setActive(item.id)}
                className={`flex items-center gap-3 rounded-2xl px-4 py-3 text-sm font-medium transition-colors ${
                  isActive ? 'bg-slate-900 text-white' : 'text-slate-600 hover:bg-slate-100'
                }`}
              >
                <Icon size={16} />
                {item.label}
              </button>
            );
          })}
        </nav>
      </aside>

      <main className="flex-1 p-6 md:p-10">
        {active === 'overview' && (
          <div className="space-y-6">
            <div className="flex flex-wrap items-center justify-between gap-4">
              <div>
                <h2 className="text-2xl font-semibold text-slate-900">Pipeline Overview</h2>
                <p className="text-sm text-slate-500">
                  Monitor the live run, approve flagged columns, and launch deterministic rule-based generations.
                </p>
              </div>
              <div className="flex flex-wrap items-center gap-3">
                <div className="rounded-full border border-slate-200 bg-white px-4 py-2 text-xs text-slate-500">
                  Active run
                </div>
                <select
                  value={selectedRunId || ''}
                  onChange={(e) => setSelectedRunId(e.target.value || null)}
                  className="rounded-full border border-slate-200 bg-white px-4 py-2 text-xs text-slate-700"
                >
                  {runs.length === 0 && <option value="">No runs yet</option>}
                  {runs.map((run) => (
                    <option key={run.run_id} value={run.run_id}>
                      {run.run_id.slice(0, 8)} | {run.source_name || 'default'} | {run.status}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div className="grid gap-4 lg:grid-cols-3">
              <div className="rounded-3xl border border-slate-200 bg-white p-5 shadow-sm">
                <div className="flex items-center gap-3 text-sm text-slate-500">
                  <Activity size={16} />
                  Run Status
                </div>
                <div className="mt-4 flex items-center justify-between gap-2">
                  <div>
                    <p className="text-lg font-semibold text-slate-900">{status?.current_step || 'Idle'}</p>
                    <p className="text-xs text-slate-500">
                      {status?.status || stats?.latest_run_status || 'idle'}
                    </p>
                  </div>
                  <span className={`rounded-full border px-3 py-1 text-xs ${STATUS_BADGE[status?.status] || STATUS_BADGE.idle}`}>
                    {status?.status || 'idle'}
                  </span>
                </div>
                <div className="mt-4 h-2 w-full overflow-hidden rounded-full bg-slate-100">
                  <div
                    className="h-full rounded-full bg-slate-900 transition-all"
                    style={{ width: `${Math.min(100, Math.max(0, status?.progress_pct || 0))}%` }}
                  ></div>
                </div>
                <div className="mt-3 flex items-center justify-between text-xs text-slate-500">
                  <span>{Math.round(status?.progress_pct || 0)}% complete</span>
                  <span className="flex items-center gap-1">
                    <Timer size={12} />
                    {formatDuration(status?.elapsed_seconds || 0)}
                  </span>
                </div>
              </div>

              <div className="rounded-3xl border border-slate-200 bg-white p-5 shadow-sm">
                <div className="flex items-center gap-3 text-sm text-slate-500">
                  <ShieldCheck size={16} />
                  Human Review Gate
                </div>
                <div className="mt-4 text-lg font-semibold text-slate-900">
                  {blockingReviews.length} blocking approvals
                </div>
                <p className="mt-2 text-xs text-slate-500">
                  Generation resumes after these columns are approved.
                </p>
                <button
                  onClick={() => setActive('review')}
                  className="mt-4 inline-flex items-center gap-2 rounded-full border border-slate-200 px-4 py-2 text-xs font-medium text-slate-700 hover:bg-slate-50"
                >
                  Review now
                  <CheckCircle size={12} />
                </button>
              </div>

              <div className="rounded-3xl border border-slate-200 bg-white p-5 shadow-sm">
                <div className="flex items-center gap-3 text-sm text-slate-500">
                  <Sparkles size={16} />
                  Validation Pass Rate
                </div>
                <div className="mt-4 text-3xl font-semibold text-slate-900">
                  {stats?.validation_pass_rate != null ? `${stats.validation_pass_rate}%` : '0%'}
                </div>
                <p className="mt-2 text-xs text-slate-500">
                  Based on the most recent validation summary.
                </p>
              </div>
            </div>

            <div className="grid gap-4 lg:grid-cols-2">
              <div className="rounded-3xl border border-slate-200 bg-white p-6 shadow-sm">
                <h3 className="text-lg font-semibold text-slate-900">Launch a new run</h3>
                <p className="mt-1 text-sm text-slate-500">
                  Optional table list for a focused test. Leave blank for the full dataset.
                </p>
                <div className="mt-4">
                  <label className="text-xs font-semibold text-slate-500">Source database</label>
                  <select
                    value={selectedSourceName}
                    onChange={(e) => setSelectedSourceName(e.target.value)}
                    className="mt-2 w-full rounded-2xl border border-slate-200 bg-white p-3 text-sm text-slate-700"
                  >
                    {loadingSources && availableSources.length === 0 && (
                      <option value="">Loading source databases...</option>
                    )}
                    {availableSources.map((source) => (
                      <option key={source.name} value={source.name}>
                        {source.label || source.name}
                        {source.table_count != null ? ` • ${source.table_count} tables` : ''}
                      </option>
                    ))}
                  </select>
                  {selectedSource?.description && (
                    <p className="mt-2 text-xs text-slate-500">
                      {selectedSource.description}
                    </p>
                  )}
                  <p className="mt-1 text-[11px] text-slate-400">
                    Available now: Telecom Source DB and Retail Mini Demo DB.
                  </p>
                </div>
                <div className="mt-4">
                  <label className="text-xs font-semibold text-slate-500">Table filter</label>
                  <textarea
                    rows={3}
                    value={tableFilterText}
                    onChange={(e) => setTableFilterText(e.target.value)}
                    placeholder="CUST_MSTR, BLNG_ACCT, INVC"
                    className="mt-2 w-full rounded-2xl border border-slate-200 bg-white p-3 text-sm text-slate-700"
                  />
                </div>
                <div className="mt-4 flex flex-wrap items-center gap-4">
                  <div className="rounded-full bg-emerald-50 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-emerald-700">
                    Rule-based only
                  </div>
                  <button
                    onClick={startPipeline}
                    disabled={starting}
                    className="ml-auto inline-flex items-center gap-2 rounded-full bg-slate-900 px-5 py-2 text-sm font-semibold text-white hover:bg-slate-800 disabled:opacity-60"
                  >
                    <PlayCircle size={16} />
                    {starting ? 'Starting...' : 'Start live run'}
                  </button>
                </div>
                {startMessage && <p className="mt-3 text-xs text-emerald-600">{startMessage}</p>}
                {startError && <p className="mt-3 text-xs text-rose-600">{startError}</p>}
              </div>

              <div className="rounded-3xl border border-slate-200 bg-white p-6 shadow-sm">
                <h3 className="text-lg font-semibold text-slate-900">Run snapshot</h3>
                <p className="mt-1 text-sm text-slate-500">
                  {activeRun ? `Run ${activeRun.run_id.slice(0, 8)} on ${activeRun.source_name || 'default'} is ${activeRun.status}.` : 'No active run selected.'}
                </p>
                <div className="mt-4 grid grid-cols-2 gap-4 text-sm">
                  <div className="rounded-2xl bg-slate-50 p-4 col-span-2">
                    <p className="text-xs text-slate-500">Source</p>
                    <p className="mt-1 text-lg font-semibold text-slate-900">{activeRun?.source_name || selectedSourceName || 'default'}</p>
                  </div>
                  <div className="rounded-2xl bg-slate-50 p-4">
                    <p className="text-xs text-slate-500">Tables</p>
                    <p className="mt-1 text-lg font-semibold text-slate-900">{stats?.total_tables || 0}</p>
                  </div>
                  <div className="rounded-2xl bg-slate-50 p-4">
                    <p className="text-xs text-slate-500">Columns</p>
                    <p className="mt-1 text-lg font-semibold text-slate-900">{stats?.total_columns || 0}</p>
                  </div>
                  <div className="rounded-2xl bg-slate-50 p-4">
                    <p className="text-xs text-slate-500">PII flagged</p>
                    <p className="mt-1 text-lg font-semibold text-slate-900">{stats?.pii_columns_detected || 0}</p>
                  </div>
                  <div className="rounded-2xl bg-slate-50 p-4">
                    <p className="text-xs text-slate-500">Pending review</p>
                    <p className="mt-1 text-lg font-semibold text-slate-900">{stats?.columns_pending_review || 0}</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {active === 'review' && (
          <div className="space-y-6">
            <div>
              <h2 className="text-2xl font-semibold text-slate-900">Human Review</h2>
              <p className="text-sm text-slate-500">
                Approve low-confidence columns to unlock generation. Only blocking items are shown here.
              </p>
            </div>

            {blockingReviews.length === 0 ? (
              <div className="rounded-3xl border border-dashed border-slate-200 bg-white p-10 text-center text-sm text-slate-500">
                No blocking items for this run.
              </div>
            ) : (
              <div className="space-y-3">
                {blockingReviews.map((item) => (
                  <div key={item.id} className="rounded-3xl border border-slate-200 bg-white p-5 shadow-sm">
                    <div className="flex flex-wrap items-center justify-between gap-4">
                      <div>
                        <p className="font-mono text-sm text-slate-900">{item.table_name}.{item.column_name}</p>
                        <p className="text-xs text-slate-500">{item.flag_reason}</p>
                      </div>
                      <span className={`rounded-full border px-3 py-1 text-xs ${item.is_blocking ? 'bg-amber-100 text-amber-700 border-amber-200' : 'bg-slate-100 text-slate-600 border-slate-200'}`}>
                        {item.is_blocking ? 'Blocking' : 'Info'}
                      </span>
                    </div>

                    {item.llm_best_guess && (
                      <div className="mt-3 text-xs text-slate-500">
                        Suggested: {item.llm_best_guess.pii_classification} | {item.llm_best_guess.masking_strategy}
                      </div>
                    )}

                    <div className="mt-4 flex flex-wrap items-center gap-3">
                      <input
                        value={reviewNotes[item.id] || ''}
                        onChange={(e) => setReviewNotes((prev) => ({ ...prev, [item.id]: e.target.value }))}
                        placeholder="Reviewer notes (optional)"
                        className="flex-1 rounded-full border border-slate-200 bg-white px-4 py-2 text-xs text-slate-700"
                      />
                      <button
                        onClick={() => approveReview(item.id)}
                        className="inline-flex items-center gap-2 rounded-full bg-slate-900 px-4 py-2 text-xs font-semibold text-white hover:bg-slate-800"
                      >
                        Approve
                        <CheckCircle size={12} />
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {active === 'insights' && (
          <GenerationLog runId={selectedRunId} embedded />
        )}

        {active === 'data' && (
          <DataViewer runId={selectedRunId} embedded />
        )}

        {active === 'graph' && (
          <GraphView embedded />
        )}

        {active === 'policies' && (
          <ReasoningPanel runId={selectedRunId} embedded />
        )}
      </main>
    </div>
  );
};

export default Dashboard;

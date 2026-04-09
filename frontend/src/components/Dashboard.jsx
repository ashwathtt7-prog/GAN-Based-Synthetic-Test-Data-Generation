import React, { useState, useEffect, useCallback } from 'react';
import axios from 'axios';
import { Play, Activity, Database, ShieldAlert, CheckCircle, Clock, RefreshCw, X, FileText, BarChart3, Network, Cpu, Table2, Brain } from 'lucide-react';
import GraphView from './GraphView';
import GenerationLog from './GenerationLog';
import DataViewer from './DataViewer';
import ReasoningPanel from './ReasoningPanel';

const API_BASE = "http://localhost:8001/api";

const Dashboard = () => {
  const [stats, setStats] = useState(null);
  const [queue, setQueue] = useState([]);
  const [runStatus, setRunStatus] = useState(null);
  const [activeRunId, setActiveRunId] = useState(null);
  const [loading, setLoading] = useState(true);
  const [policies, setPolicies] = useState([]);
  const [showPolicies, setShowPolicies] = useState(false);
  const [showGraph, setShowGraph] = useState(false);
  const [showGenLog, setShowGenLog] = useState(false);
  const [showDataViewer, setShowDataViewer] = useState(false);
  const [showReasoning, setShowReasoning] = useState(false);
  const [correctionModal, setCorrectionModal] = useState(null);
  const [correctionData, setCorrectionData] = useState('');
  const [abbreviationModal, setAbbreviationModal] = useState(null);
  const [abbrToken, setAbbrToken] = useState('');
  const [abbrExpansion, setAbbrExpansion] = useState('');

  const fetchDashboardData = useCallback(async () => {
    try {
      const statsRes = await axios.get(`${API_BASE}/dashboard/stats`);
      setStats(statsRes.data);

      const queueRes = await axios.get(`${API_BASE}/review/queue`);
      setQueue(queueRes.data);

      // Poll active run if we have one, or discover running pipelines
      let runId = activeRunId;
      if (!runId && statsRes.data.latest_run_status === 'running') {
        // Discover a running pipeline we didn't start from this dashboard session
        try {
          const runsRes = await axios.get(`${API_BASE}/pipeline/runs`);
          const running = runsRes.data.find(r => r.status === 'running');
          if (running) {
            runId = running.run_id;
            setActiveRunId(runId);
          }
        } catch (err) { /* runs endpoint may not exist */ }
      }
      if (runId) {
        try {
          const statusRes = await axios.get(`${API_BASE}/pipeline/status/${runId}`);
          setRunStatus(statusRes.data);
          if (statusRes.data.status === 'completed' || statusRes.data.status === 'failed') {
            // Keep showing final status
          }
        } catch (err) {
          // Run may not exist yet
        }
      }
    } catch (err) {
      console.error("Failed to load dashboard data", err);
    } finally {
      setLoading(false);
    }
  }, [activeRunId]);

  useEffect(() => {
    fetchDashboardData();
    const interval = setInterval(fetchDashboardData, 3000);
    return () => clearInterval(interval);
  }, [fetchDashboardData]);

  const startPipeline = async () => {
    try {
      const res = await axios.post(`${API_BASE}/pipeline/start`);
      setActiveRunId(res.data.run_id);
      fetchDashboardData();
    } catch (err) {
      console.error("Failed to start pipeline", err);
    }
  };

  const handleApprove = async (id) => {
    try {
      await axios.post(`${API_BASE}/review/${id}/approve`, { reviewer_notes: "Approved from Dashboard" });
      fetchDashboardData();
    } catch (err) {
      console.error("Failed to approve", err);
    }
  };

  const handleCorrect = async (id) => {
    try {
      const correctedPolicy = JSON.parse(correctionData);
      await axios.post(`${API_BASE}/review/${id}/correct`, {
        corrected_policy: correctedPolicy,
        reviewer_notes: "Corrected from Dashboard"
      });
      setCorrectionModal(null);
      setCorrectionData('');
      fetchDashboardData();
    } catch (err) {
      console.error("Failed to correct", err);
      alert("Invalid JSON. Please check your correction data.");
    }
  };

  const handleAbbreviation = async (id) => {
    try {
      await axios.post(`${API_BASE}/review/${id}/abbreviation`, {
        token: abbrToken,
        expansion: abbrExpansion,
        reviewer_notes: "Submitted from Dashboard"
      });
      setAbbreviationModal(null);
      setAbbrToken('');
      setAbbrExpansion('');
      fetchDashboardData();
    } catch (err) {
      console.error("Failed to submit abbreviation", err);
    }
  };

  const fetchPolicies = async () => {
    try {
      const res = await axios.get(`${API_BASE}/policies`);
      setPolicies(res.data);
      setShowPolicies(true);
    } catch (err) {
      console.error("Failed to fetch policies", err);
    }
  };

  if (loading && !stats) return <div className="p-8">Loading Dashboard...</div>;

  const isRunning = runStatus && runStatus.status === 'running';
  const progressPct = runStatus?.progress_pct || 0;

  return (
    <div className="p-8 max-w-7xl mx-auto">
      {/* Header */}
      <div className="flex justify-between items-center mb-8">
        <div>
          <h1 className="text-3xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-emerald-400">
            GAN Synthetic Data Orchestrator
          </h1>
          <p className="text-gray-400 mt-2">Telecom Enterprise — Semantic Graph & SDV Integration</p>
        </div>
        <div className="flex gap-2 flex-wrap">
          <button
            onClick={() => setShowGraph(true)}
            className="flex items-center gap-2 bg-gray-700 hover:bg-gray-600 px-3 py-2.5 rounded-lg font-medium text-sm transition-all"
          >
            <Network size={16} />
            Graph
          </button>
          <button
            onClick={() => setShowReasoning(true)}
            className="flex items-center gap-2 bg-gray-700 hover:bg-gray-600 px-3 py-2.5 rounded-lg font-medium text-sm transition-all"
          >
            <Brain size={16} />
            LLM Reasoning
          </button>
          <button
            onClick={() => setShowGenLog(true)}
            className="flex items-center gap-2 bg-gray-700 hover:bg-gray-600 px-3 py-2.5 rounded-lg font-medium text-sm transition-all"
          >
            <Cpu size={16} />
            Generation Insights
          </button>
          <button
            onClick={() => setShowDataViewer(true)}
            className="flex items-center gap-2 bg-gray-700 hover:bg-gray-600 px-3 py-2.5 rounded-lg font-medium text-sm transition-all"
          >
            <Table2 size={16} />
            View Data
          </button>
          <button
            onClick={fetchPolicies}
            className="flex items-center gap-2 bg-gray-700 hover:bg-gray-600 px-3 py-2.5 rounded-lg font-medium text-sm transition-all"
          >
            <FileText size={16} />
            Policies
          </button>
          <button
            onClick={startPipeline}
            disabled={isRunning}
            className={`flex items-center gap-2 ${isRunning ? 'bg-gray-600 cursor-not-allowed' : 'bg-telecom-accent hover:bg-blue-600'} px-6 py-3 rounded-lg font-semibold transition-all transform hover:scale-105 shadow-md shadow-blue-500/20`}
          >
            {isRunning ? <RefreshCw size={20} className="animate-spin" /> : <Play size={20} />}
            {isRunning ? 'Running...' : 'Execute Pipeline'}
          </button>
        </div>
      </div>

      {/* Stat Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-6 mb-8">
        <StatCard icon={<Database />} title="Tables Ingested" value={stats?.total_tables || 0} />
        <StatCard icon={<CheckCircle />} title="Columns Classified" value={stats?.columns_classified || 0} />
        <StatCard icon={<ShieldAlert />} title="PII Attributes" value={stats?.pii_columns_detected || 0} className="text-red-400" />
        <StatCard icon={<Clock />} title="Pending Reviews" value={stats?.columns_pending_review || 0} className="text-yellow-400" />
        <StatCard icon={<BarChart3 />} title="Validation Pass" value={`${stats?.validation_pass_rate || 0}%`} className="text-green-400" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Pipeline Status */}
        <div className="lg:col-span-2 bg-telecom-card p-6 rounded-xl border border-gray-800 shadow-xl">
          <h2 className="text-xl font-bold mb-4 flex items-center gap-2">
            <Activity className="text-blue-400" />
            Live Execution Status
          </h2>
          {!runStatus || runStatus.status === 'idle' ? (
            <div className="text-gray-500 italic py-8 text-center border border-dashed border-gray-700 rounded-lg">
              No active runs. Click "Execute Pipeline" to start.
            </div>
          ) : (
            <div>
              <div className="mb-2 flex justify-between">
                <span className="font-semibold text-gray-300">
                  {runStatus.current_step || "Processing..."}
                </span>
                <span className={`font-bold ${runStatus.status === 'completed' ? 'text-green-400' : runStatus.status === 'failed' ? 'text-red-400' : 'text-blue-400'}`}>
                  {Math.round(progressPct)}%
                </span>
              </div>
              <div className="w-full bg-gray-800 rounded-full h-3 mb-4 overflow-hidden">
                <div
                  className={`h-3 rounded-full transition-all duration-500 ${runStatus.status === 'failed' ? 'bg-red-500' : 'bg-gradient-to-r from-blue-500 to-emerald-500'}`}
                  style={{ width: `${progressPct}%` }}
                ></div>
              </div>

              <div className="flex justify-between text-sm text-gray-400 mb-4">
                <span>Status: <span className={`font-semibold ${runStatus.status === 'completed' ? 'text-green-400' : runStatus.status === 'failed' ? 'text-red-400' : 'text-yellow-400'}`}>{runStatus.status?.toUpperCase()}</span></span>
                <span>Elapsed: {Math.round(runStatus.elapsed_seconds || 0)}s</span>
              </div>

              {/* Domain Progress */}
              {(runStatus.domains_completed?.length > 0 || runStatus.domains_pending?.length > 0) && (
                <div className="mt-4">
                  <p className="text-sm text-gray-400 mb-2">Domains:</p>
                  <div className="flex flex-wrap gap-2">
                    {runStatus.domains_completed?.map(d => (
                      <span key={d} className="bg-green-500/20 text-green-400 px-3 py-1 rounded-full text-xs">{d} ✓</span>
                    ))}
                    {runStatus.domains_pending?.map(d => (
                      <span key={d} className="bg-gray-700 text-gray-400 px-3 py-1 rounded-full text-xs">{d}</span>
                    ))}
                  </div>
                </div>
              )}

              {/* Phase indicators */}
              <div className="grid grid-cols-4 gap-3 text-sm mt-6">
                <PhaseCard label="Ingestion" active={progressPct >= 2} complete={progressPct >= 18} />
                <PhaseCard label="Intelligence" active={progressPct >= 25} complete={progressPct >= 65} />
                <PhaseCard label="Generation" active={progressPct >= 65} complete={progressPct >= 82} />
                <PhaseCard label="Validation" active={progressPct >= 82} complete={progressPct >= 95} />
              </div>
            </div>
          )}
        </div>

        {/* Human Review Queue */}
        <div className="bg-telecom-card p-6 rounded-xl border border-gray-800 shadow-xl overflow-hidden flex flex-col h-[560px]">
          <h2 className="text-xl font-bold mb-4 flex items-center gap-2">
            <ShieldAlert className="text-yellow-400" />
            Human Review Queue ({queue.length})
          </h2>

          <div className="overflow-y-auto flex-1 pr-2">
            {queue.length === 0 ? (
              <div className="text-gray-500 italic text-center py-8">
                All semantic classifications look confident!
              </div>
            ) : (
              <div className="space-y-4">
                {queue.map(item => (
                  <div key={item.id} className="bg-gray-800/50 p-4 rounded-lg border border-gray-700">
                    <div className="flex justify-between items-start mb-2">
                      <span className="font-semibold text-blue-300">{item.table_name}</span>
                      <span className="text-xs bg-red-500/20 text-red-400 px-2 py-1 rounded">
                        {item.flag_reason}
                      </span>
                    </div>
                    <p className="font-mono text-sm mb-3">{item.column_name}</p>
                    <div className="bg-gray-900 p-2 rounded text-xs text-gray-400 mb-3 overflow-x-auto max-h-24">
                      <pre>{JSON.stringify(item.llm_best_guess, null, 2)}</pre>
                    </div>
                    <div className="flex gap-2">
                      <button
                        onClick={() => handleApprove(item.id)}
                        className="flex-1 bg-green-600/20 hover:bg-green-600/40 text-green-400 py-1.5 rounded text-sm transition-colors"
                      >
                        Approve
                      </button>
                      <button
                        onClick={() => {
                          setCorrectionModal(item);
                          setCorrectionData(JSON.stringify(item.llm_best_guess, null, 2));
                        }}
                        className="flex-1 bg-blue-600/20 hover:bg-blue-600/40 text-blue-400 py-1.5 rounded text-sm transition-colors"
                      >
                        Correct
                      </button>
                      {item.flag_reason === 'ABBREVIATION_UNKNOWN' && (
                        <button
                          onClick={() => {
                            setAbbreviationModal(item);
                            setAbbrToken(item.column_name?.split('_').pop() || '');
                          }}
                          className="flex-1 bg-purple-600/20 hover:bg-purple-600/40 text-purple-400 py-1.5 rounded text-sm transition-colors"
                        >
                          Abbreviation
                        </button>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Policies Table Modal */}
      {showPolicies && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-8">
          <div className="bg-gray-900 rounded-xl border border-gray-700 w-full max-w-6xl max-h-[80vh] overflow-hidden flex flex-col">
            <div className="flex justify-between items-center p-6 border-b border-gray-700">
              <h2 className="text-xl font-bold">Column Policies ({policies.length})</h2>
              <button onClick={() => setShowPolicies(false)} className="text-gray-400 hover:text-white">
                <X size={24} />
              </button>
            </div>
            <div className="overflow-auto flex-1 p-4">
              <table className="w-full text-sm">
                <thead className="text-gray-400 border-b border-gray-700">
                  <tr>
                    <th className="text-left p-2">Table</th>
                    <th className="text-left p-2">Column</th>
                    <th className="text-left p-2">PII</th>
                    <th className="text-left p-2">Source</th>
                    <th className="text-left p-2">Masking</th>
                    <th className="text-left p-2">Importance</th>
                    <th className="text-left p-2">Confidence</th>
                    <th className="text-left p-2">Dedup</th>
                  </tr>
                </thead>
                <tbody>
                  {policies.map(p => (
                    <tr key={p.id} className="border-b border-gray-800 hover:bg-gray-800/50">
                      <td className="p-2 font-mono text-blue-300">{p.table_name}</td>
                      <td className="p-2 font-mono">{p.column_name}</td>
                      <td className="p-2">
                        <span className={`px-2 py-0.5 rounded text-xs ${p.pii_classification === 'none' ? 'bg-gray-700' : 'bg-red-500/20 text-red-400'}`}>
                          {p.pii_classification}
                        </span>
                      </td>
                      <td className="p-2 text-gray-400">{p.pii_source}</td>
                      <td className="p-2">
                        <span className={`px-2 py-0.5 rounded text-xs ${
                          p.masking_strategy === 'passthrough' ? 'bg-green-500/20 text-green-400' :
                          p.masking_strategy === 'suppress' ? 'bg-red-500/20 text-red-400' :
                          'bg-yellow-500/20 text-yellow-400'
                        }`}>
                          {p.masking_strategy}
                        </span>
                      </td>
                      <td className="p-2">{p.business_importance}</td>
                      <td className="p-2">{p.llm_confidence?.toFixed(2)}</td>
                      <td className="p-2 text-gray-400">{p.dedup_mode}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* Correction Modal */}
      {correctionModal && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-8">
          <div className="bg-gray-900 rounded-xl border border-gray-700 w-full max-w-2xl">
            <div className="flex justify-between items-center p-6 border-b border-gray-700">
              <h2 className="text-xl font-bold">Correct Policy: {correctionModal.table_name}.{correctionModal.column_name}</h2>
              <button onClick={() => setCorrectionModal(null)} className="text-gray-400 hover:text-white">
                <X size={24} />
              </button>
            </div>
            <div className="p-6">
              <p className="text-sm text-gray-400 mb-3">Edit the JSON policy below:</p>
              <textarea
                className="w-full h-64 bg-gray-800 text-gray-200 font-mono text-sm p-4 rounded-lg border border-gray-700 focus:border-blue-500 focus:outline-none"
                value={correctionData}
                onChange={(e) => setCorrectionData(e.target.value)}
              />
              <div className="flex gap-3 mt-4">
                <button
                  onClick={() => handleCorrect(correctionModal.id)}
                  className="flex-1 bg-blue-600 hover:bg-blue-700 py-2 rounded-lg font-semibold transition-colors"
                >
                  Submit Correction
                </button>
                <button
                  onClick={() => setCorrectionModal(null)}
                  className="flex-1 bg-gray-700 hover:bg-gray-600 py-2 rounded-lg font-semibold transition-colors"
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Knowledge Graph View */}
      {showGraph && <GraphView onClose={() => setShowGraph(false)} />}

      {/* Generation Tier View */}
      {showGenLog && <GenerationLog onClose={() => setShowGenLog(false)} />}

      {/* Data Viewer */}
      {showDataViewer && <DataViewer onClose={() => setShowDataViewer(false)} />}

      {/* LLM Reasoning Panel */}
      {showReasoning && <ReasoningPanel onClose={() => setShowReasoning(false)} />}

      {/* Abbreviation Modal */}
      {abbreviationModal && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-8">
          <div className="bg-gray-900 rounded-xl border border-gray-700 w-full max-w-md">
            <div className="flex justify-between items-center p-6 border-b border-gray-700">
              <h2 className="text-xl font-bold">Resolve Abbreviation</h2>
              <button onClick={() => setAbbreviationModal(null)} className="text-gray-400 hover:text-white">
                <X size={24} />
              </button>
            </div>
            <div className="p-6">
              <p className="text-sm text-gray-400 mb-4">
                Column: <span className="font-mono text-blue-300">{abbreviationModal.column_name}</span>
              </p>
              <div className="mb-4">
                <label className="text-sm text-gray-400 block mb-1">Abbreviation Token</label>
                <input
                  className="w-full bg-gray-800 text-gray-200 font-mono p-3 rounded-lg border border-gray-700 focus:border-purple-500 focus:outline-none"
                  value={abbrToken}
                  onChange={(e) => setAbbrToken(e.target.value)}
                  placeholder="e.g., CUST"
                />
              </div>
              <div className="mb-4">
                <label className="text-sm text-gray-400 block mb-1">Expansion</label>
                <input
                  className="w-full bg-gray-800 text-gray-200 p-3 rounded-lg border border-gray-700 focus:border-purple-500 focus:outline-none"
                  value={abbrExpansion}
                  onChange={(e) => setAbbrExpansion(e.target.value)}
                  placeholder="e.g., Customer"
                />
              </div>
              <button
                onClick={() => handleAbbreviation(abbreviationModal.id)}
                className="w-full bg-purple-600 hover:bg-purple-700 py-2 rounded-lg font-semibold transition-colors"
              >
                Save Abbreviation
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

const StatCard = ({ icon, title, value, className = "text-telecom-accent" }) => (
  <div className="bg-telecom-card p-5 rounded-xl border border-gray-800 flex items-center gap-4">
    <div className={`p-3 bg-gray-800 rounded-lg ${className}`}>
      {icon}
    </div>
    <div>
      <p className="text-gray-400 text-sm mb-1">{title}</p>
      <p className="text-2xl font-bold">{value}</p>
    </div>
  </div>
);

const PhaseCard = ({ label, active, complete }) => (
  <div className={`p-3 rounded-lg text-center text-xs ${
    complete ? 'bg-green-500/10 border border-green-500/30' :
    active ? 'bg-blue-500/10 border border-blue-500/30' :
    'bg-gray-800/50 border border-gray-700'
  }`}>
    <p className="text-gray-400 mb-1">{label}</p>
    <p className={`font-semibold ${
      complete ? 'text-green-400' : active ? 'text-blue-400' : 'text-gray-500'
    }`}>
      {complete ? '✓ Done' : active ? 'Running' : 'Pending'}
    </p>
  </div>
);

export default Dashboard;

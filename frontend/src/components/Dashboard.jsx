import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Play, Activity, Database, ShieldAlert, CheckCircle, Clock } from 'lucide-react';

const API_BASE = "http://localhost:8000/api";

const Dashboard = () => {
  const [stats, setStats] = useState(null);
  const [queue, setQueue] = useState([]);
  const [runStatus, setRunStatus] = useState(null);
  const [loading, setLoading] = useState(true);

  const fetchDashboardData = async () => {
    try {
      const statsRes = await axios.get(`${API_BASE}/dashboard/stats`);
      setStats(statsRes.data);
      
      const queueRes = await axios.get(`${API_BASE}/review/queue`);
      setQueue(queueRes.data);
      
      if (statsRes.data.latest_run_status && statsRes.data.latest_run_status !== 'idle') {
          // Poll current run
      }
      
    } catch (err) {
      console.error("Failed to load dashboard data", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDashboardData();
    const interval = setInterval(fetchDashboardData, 5000);
    return () => clearInterval(interval);
  }, []);

  const startPipeline = async () => {
    try {
      const res = await axios.post(`${API_BASE}/pipeline/start`);
      console.log("Pipeline started", res.data);
      fetchDashboardData(); // Refresh immediately
    } catch (err) {
      console.error("Failed to start pipeline", err);
    }
  };

  const handleApprove = async (id) => {
    try {
      await axios.post(`${API_BASE}/review/${id}/approve`, { reviewer_notes: "Auto-approved from Dashboard" });
      fetchDashboardData();
    } catch (err) {
      console.error("Failed to approve", err);
    }
  };

  if (loading && !stats) return <div className="p-8">Loading Dashboard...</div>;

  return (
    <div className="p-8 max-w-7xl mx-auto">
      <div className="flex justify-between items-center mb-8">
        <div>
          <h1 className="text-3xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-emerald-400">
            GAN Synthetic Data Orchestrator
          </h1>
          <p className="text-gray-400 mt-2">Telecom Enterprise Scale - Semantic Graph & SDV Integration</p>
        </div>
        <button 
          onClick={startPipeline}
          className="flex items-center gap-2 bg-telecom-accent hover:bg-blue-600 px-6 py-3 rounded-lg font-semibold transition-all transform hover:scale-105 shadow-md shadow-blue-500/20"
        >
          <Play size={20} />
          Execute Pipeline
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <StatCard icon={<Database />} title="Tables Ingested" value={stats?.total_tables || 0} />
        <StatCard icon={<CheckCircle />} title="Columns Classified" value={stats?.columns_classified || 0} />
        <StatCard icon={<ShieldAlert />} title="PII Attributes" value={stats?.pii_columns_detected || 0} className="text-red-400" />
        <StatCard icon={<Clock />} title="Pending Reviews" value={stats?.columns_pending_review || 0} className="text-yellow-400" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Active Pipeline Status */}
        <div className="lg:col-span-2 bg-telecom-card p-6 rounded-xl border border-gray-800 shadow-xl">
          <h2 className="text-xl font-bold mb-4 flex items-center gap-2">
            <Activity className="text-blue-400" />
            Live Execution Status
          </h2>
          {stats?.latest_run_status === 'idle' || !stats?.latest_run_status ? (
            <div className="text-gray-500 italic py-8 text-center border border-dashed border-gray-700 rounded-lg">
              No active runs. Schema graphs are idle.
            </div>
          ) : (
            <div>
               <div className="mb-2 flex justify-between">
                 <span className="font-semibold text-gray-300">Phase: {runStatus?.current_step || "Processing..."}</span>
                 <span>{Math.round(runStatus?.progress_pct || stats?.validation_pass_rate || 0)}%</span>
               </div>
               <div className="w-full bg-gray-800 rounded-full h-3 mb-6 overflow-hidden">
                 <div className="bg-gradient-to-r from-blue-500 to-emerald-500 h-3 rounded-full transition-all duration-500" 
                      style={{ width: `${runStatus?.progress_pct || stats?.validation_pass_rate || 50}%` }}></div>
               </div>
               
               <div className="grid grid-cols-3 gap-4 text-sm mt-8">
                  <div className="p-4 bg-gray-800/50 rounded-lg text-center">
                    <p className="text-gray-400 mb-1">Knowledge Graph</p>
                    <p className="text-green-400 font-semibold">Active</p>
                  </div>
                  <div className="p-4 bg-gray-800/50 rounded-lg text-center">
                    <p className="text-gray-400 mb-1">Semantic LLM</p>
                    <p className="text-green-400 font-semibold">Online</p>
                  </div>
                  <div className="p-4 bg-gray-800/50 rounded-lg text-center">
                    <p className="text-gray-400 mb-1">CTGAN Node</p>
                    <p className="text-gray-500 font-semibold">Waiting...</p>
                  </div>
               </div>
            </div>
          )}
        </div>

        {/* Human Review Queue */}
        <div className="bg-telecom-card p-6 rounded-xl border border-gray-800 shadow-xl overflow-hidden flex flex-col h-[500px]">
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
                    <div className="bg-gray-900 p-2 rounded text-xs text-gray-400 mb-3 overflow-x-auto">
                        <pre>{JSON.stringify(item.llm_best_guess, null, 2)}</pre>
                    </div>
                    <div className="flex gap-2">
                      <button onClick={() => handleApprove(item.id)} className="flex-1 bg-green-600/20 hover:bg-green-600/40 text-green-400 py-1.5 rounded text-sm transition-colors">
                        Approve
                      </button>
                      <button className="flex-1 bg-blue-600/20 hover:bg-blue-600/40 text-blue-400 py-1.5 rounded text-sm transition-colors">
                        Correct
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

const StatCard = ({ icon, title, value, className = "text-telecom-accent" }) => (
  <div className="bg-telecom-card p-6 rounded-xl border border-gray-800 flex items-center gap-4">
    <div className={`p-4 bg-gray-800 rounded-lg ${className}`}>
      {icon}
    </div>
    <div>
      <p className="text-gray-400 text-sm mb-1">{title}</p>
      <p className="text-3xl font-bold">{value}</p>
    </div>
  </div>
);

export default Dashboard;

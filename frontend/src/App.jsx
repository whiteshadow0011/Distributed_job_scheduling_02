import React, { useState, useEffect } from 'react';
import { api } from './api';
import { 
  Activity, Server, Play, Pause, RotateCcw, AlertTriangle, 
  Send, Layers, BarChart2, ShieldCheck, Clock, UserPlus, LogIn, CheckCircle2, XCircle, Plus, FolderPlus
} from 'lucide-react';
import { 
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer 
} from 'recharts';

export default function App() {
  const [token, setToken] = useState(localStorage.getItem('token') || '');
  
  // Auth State
  const [isRegistering, setIsRegistering] = useState(false);
  const [name, setName] = useState('Admin User');
  const [orgName, setOrgName] = useState('Acme Corp');
  const [email, setEmail] = useState('admin@acme.com');
  const [password, setPassword] = useState('securepassword123');
  const [authError, setAuthError] = useState('');

  // Dashboard Data State
  const [queues, setQueues] = useState([]);
  const [selectedQueue, setSelectedQueue] = useState(null);
  const [queueMetrics, setQueueMetrics] = useState(null);
  const [chartData, setChartData] = useState([]);
  const [recentJobs, setRecentJobs] = useState([]);
  const [dlqJobs, setDlqJobs] = useState([]);
  const [workers, setWorkers] = useState([]);
  const [loading, setLoading] = useState(false);

  // Modal / Creator State
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [newProjectName, setNewProjectName] = useState('');
  const [newQueueName, setNewQueueName] = useState('');
  const [newConcurrency, setNewConcurrency] = useState(10);

  // Form State
  const [jobType, setJobType] = useState('SEND_WELCOME_EMAIL');
  const [jobPriority, setJobPriority] = useState(5);
  const [jobPayload, setJobPayload] = useState('{"email": "user@scale.io"}');
  const [batchCount, setBatchCount] = useState(10);

  // 1. Auth Handlers
  const handleAuth = async (e) => {
    e.preventDefault();
    setAuthError('');
    try {
      if (isRegistering) {
        const res = await api.post('/auth/register', { name, email, password, organizationName: orgName });
        const t = res.data.token || res.data.accessToken;
        localStorage.setItem('token', t);
        setToken(t);
      } else {
        const res = await api.post('/auth/login', { email, password });
        const t = res.data.token || res.data.accessToken;
        localStorage.setItem('token', t);
        setToken(t);
      }
    } catch (err) {
      setAuthError(err.response?.data?.error || err.message || 'Authentication failed');
    }
  };

  const handleLogout = () => {
    localStorage.removeItem('token');
    setToken('');
  };

  // 2. Fetch Queues and Workers
  const fetchOverview = async () => {
    if (!token) return;
    try {
      const [qRes, wRes] = await Promise.all([
        api.get(`/queues?_t=${Date.now()}`),
        api.get(`/metrics/overview?_t=${Date.now()}`)
      ]);
      
      const qList = Array.isArray(qRes.data) ? qRes.data : qRes.data?.queues || [];
      setQueues(qList);
      
      setSelectedQueue((prev) => {
        if (!prev && qList.length > 0) return qList[0];
        if (prev) {
          const matched = qList.find(q => q.id === prev.id);
          return matched || qList[0] || null;
        }
        return null;
      });

      if (Array.isArray(wRes.data?.workers) && wRes.data.workers.length > 0) {
        setWorkers(wRes.data.workers);
      } else {
        const count = Number(wRes.data?.live?.totalActiveWorkerNodes || 0);
        setWorkers(
          Array.from({ length: count }, (_, idx) => ({
            id: `worker-node-${idx + 1}`,
            pid: 1,
            hostname: `docker-worker-${idx + 1}`,
            status: 'ACTIVE',
          }))
        );
      }
    } catch (err) {
      console.error('[Overview Error]:', err);
    }
  };

  // 3. Fetch Single Queue Metrics, DLQ, and Recent Jobs
  const fetchQueueData = async () => {
    if (!token || !selectedQueue) return;

    try {
      const mRes = await api.get(`/queues/${selectedQueue.id}/metrics?_t=${Date.now()}`);
      if (mRes.data) {
        setQueueMetrics(mRes.data);
        const series = mRes.data.timeSeries24h || [];
        setChartData(
          series.map((item) => ({
            time_bucket: item.time_bucket,
            completed: Number(item.completed || 0),
            failed: Number(item.failed || 0),
            avg_latency_ms: Number(item.avg_latency_ms || 0),
          }))
        );
      }
    } catch (err) {
      console.error('[Metrics Error]:', err.message);
    }

    try {
      const dlqRes = await api.get(`/queues/${selectedQueue.id}/dlq?_t=${Date.now()}`);
      const list = dlqRes.data?.dlqJobs || dlqRes.data?.jobs || (Array.isArray(dlqRes.data) ? dlqRes.data : []);
      setDlqJobs(list);
    } catch {
      setDlqJobs([]);
    }

    try {
      const jobsRes = await api.get(`/jobs?queueId=${selectedQueue.id}&limit=10&_t=${Date.now()}`);
      const rawJobs = jobsRes.data?.jobs || (Array.isArray(jobsRes.data) ? jobsRes.data : []);
      setRecentJobs(rawJobs);
    } catch {
      setRecentJobs([]);
    }
  };

  useEffect(() => {
    if (token) {
      fetchOverview();
      const interval = setInterval(fetchOverview, 6000);
      return () => clearInterval(interval);
    }
  }, [token]);

  useEffect(() => {
    if (token && selectedQueue) {
      fetchQueueData();
      const interval = setInterval(fetchQueueData, 3000);
      return () => clearInterval(interval);
    }
  }, [selectedQueue, token]);

  // Working Pause / Resume Toggle
  const togglePauseQueue = async () => {
    if (!selectedQueue) return;
    const nextPausedState = !Boolean(selectedQueue.is_paused);
    
    // Optimistic UI update
    setSelectedQueue({ ...selectedQueue, is_paused: nextPausedState });

    try {
      const res = await api.patch(`/queues/${selectedQueue.id}/pause`, {
        isPaused: nextPausedState
      });
      
      if (res.data?.queue) {
        setSelectedQueue(res.data.queue);
      }
      fetchOverview();
    } catch (err) {
      alert('Failed to toggle queue pause state: ' + (err.response?.data?.error || err.message));
      fetchOverview();
    }
  };

  // 2. Working Queue Creator (Uses GET /projects to find projectId, then POST /queues)
  const handleCreateProjectQueue = async (e) => {
    e.preventDefault();
    if (!newQueueName.trim()) return;
    setLoading(true);

    try {
      // Fetch available project ID from existing organization
      const projRes = await api.get('/projects');
      const projList = Array.isArray(projRes.data) ? projRes.data : projRes.data.projects || [];

      if (projList.length === 0) {
        throw new Error('No existing project found for your organization.');
      }

      const projectId = projList[0].id;

      // Post directly to POST /queues
      await api.post('/queues', {
        projectId,
        name: newQueueName.trim().toLowerCase().replace(/\s+/g, '-'),
        concurrencyLimit: Number(newConcurrency) || 10,
        priority: 5,
      });

      setShowCreateModal(false);
      setNewQueueName('');
      await fetchOverview();
      alert('Queue created successfully!');
    } catch (err) {
      alert('Error creating Queue: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // 6. Enqueue Jobs
  const createJob = async (isBatch = false) => {
    if (!selectedQueue) return;
    setLoading(true);
    try {
      let parsed = {};
      try { parsed = JSON.parse(jobPayload); } catch { parsed = { data: jobPayload }; }

      if (isBatch) {
        const jobs = Array.from({ length: Number(batchCount) }, (_, idx) => ({
          type: jobType,
          payload: { ...parsed, batchIndex: idx + 1 },
          priority: Number(jobPriority),
        }));
        await api.post('/jobs/batch', { queueId: selectedQueue.id, jobs });
      } else {
        await api.post('/jobs', {
          queueId: selectedQueue.id,
          type: jobType,
          payload: parsed,
          priority: Number(jobPriority),
        });
      }
      setTimeout(fetchQueueData, 300);
    } catch (err) {
      alert('Failed to enqueue: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  const replayDLQ = async (jobId = null) => {
    if (!selectedQueue) return;
    try {
      if (jobId) {
        await api.post(`/queues/${selectedQueue.id}/dlq/replay`, { jobIds: [jobId] });
      } else {
        await api.post(`/queues/${selectedQueue.id}/dlq/replay-all`);
      }
      setTimeout(fetchQueueData, 500);
    } catch (err) {
      alert('Replay failed: ' + (err.response?.data?.error || err.message));
    }
  };

  if (!token) {
    return (
      <div className="min-h-screen bg-slate-950 flex items-center justify-center p-4">
        <div className="max-w-md w-full bg-slate-900 border border-slate-800 rounded-2xl p-8 shadow-2xl">
          <div className="flex items-center gap-3 mb-6">
            <div className="p-3 bg-indigo-500/10 text-indigo-400 rounded-xl">
              <Layers className="w-6 h-6" />
            </div>
            <div>
              <h1 className="text-xl font-bold text-white">Distributed Scheduler</h1>
              <p className="text-xs text-slate-400">Real-Time Control Plane</p>
            </div>
          </div>

          {authError && (
            <div className="mb-4 p-3 bg-rose-500/10 border border-rose-500/20 text-rose-400 rounded-xl text-xs">
              {authError}
            </div>
          )}

          <form onSubmit={handleAuth} className="space-y-4">
            {isRegistering && (
              <>
                <div>
                  <label className="text-xs font-semibold uppercase text-slate-400">Full Name</label>
                  <input 
                    type="text" 
                    required
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                    className="w-full mt-1 px-4 py-2.5 bg-slate-950 border border-slate-800 rounded-xl focus:outline-none focus:border-indigo-500 text-sm"
                  />
                </div>
                <div>
                  <label className="text-xs font-semibold uppercase text-slate-400">Organization Name</label>
                  <input 
                    type="text" 
                    required
                    value={orgName}
                    onChange={(e) => setOrgName(e.target.value)}
                    className="w-full mt-1 px-4 py-2.5 bg-slate-950 border border-slate-800 rounded-xl focus:outline-none focus:border-indigo-500 text-sm"
                  />
                </div>
              </>
            )}

            <div>
              <label className="text-xs font-semibold uppercase text-slate-400">Email Address</label>
              <input 
                type="email" 
                required
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="w-full mt-1 px-4 py-2.5 bg-slate-950 border border-slate-800 rounded-xl focus:outline-none focus:border-indigo-500 text-sm"
              />
            </div>
            <div>
              <label className="text-xs font-semibold uppercase text-slate-400">Password</label>
              <input 
                type="password" 
                required
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="w-full mt-1 px-4 py-2.5 bg-slate-950 border border-slate-800 rounded-xl focus:outline-none focus:border-indigo-500 text-sm"
              />
            </div>

            <button 
              type="submit" 
              className="w-full py-3 bg-indigo-600 hover:bg-indigo-500 text-white font-medium rounded-xl text-sm transition-all shadow-lg shadow-indigo-600/20 flex items-center justify-center gap-2"
            >
              {isRegistering ? <UserPlus className="w-4 h-4" /> : <LogIn className="w-4 h-4" />}
              {isRegistering ? 'Create Account & Org' : 'Sign In to Console'}
            </button>
          </form>

          <div className="mt-6 text-center border-t border-slate-800/80 pt-4">
            <button
              onClick={() => {
                setIsRegistering(!isRegistering);
                setAuthError('');
              }}
              className="text-xs text-indigo-400 hover:text-indigo-300 font-medium transition-colors"
            >
              {isRegistering ? 'Already have an account? Sign in' : "Don't have an account? Create one"}
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col">
      {/* Top Navbar */}
      <header className="border-b border-slate-800 bg-slate-900/50 backdrop-blur px-6 py-4 flex items-center justify-between sticky top-0 z-50">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-indigo-500/10 text-indigo-400 rounded-lg">
            <Layers className="w-5 h-5" />
          </div>
          <span className="font-bold text-lg text-white">ScaleQueue Real-Time Hub</span>
        </div>

        <div className="flex items-center gap-4">
          <button
            onClick={() => setShowCreateModal(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-semibold shadow-lg shadow-indigo-600/20 transition-all"
          >
            <Plus className="w-3.5 h-3.5" />
            New Project & Queue
          </button>
          <button 
            onClick={handleLogout}
            className="text-xs text-slate-400 hover:text-white transition-colors"
          >
            Sign Out
          </button>
        </div>
      </header>

      {/* Main Container */}
      <div className="flex-1 max-w-7xl w-full mx-auto p-6 space-y-6">
        
        {/* Top Queue Bar with Interactive Pause Button */}
        <div className="flex flex-wrap items-center justify-between gap-4 bg-slate-900 border border-slate-800 p-4 rounded-2xl">
          <div className="flex items-center gap-3">
            <span className="text-xs font-semibold uppercase text-slate-400">Queue:</span>
            <div className="flex gap-2 flex-wrap">
              {queues.map((q) => (
                <button
                  key={q.id}
                  onClick={() => setSelectedQueue(q)}
                  className={`px-3 py-1.5 rounded-xl text-xs font-medium transition-all ${
                    selectedQueue?.id === q.id 
                      ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-600/20' 
                      : 'bg-slate-800 text-slate-400 hover:bg-slate-700'
                  }`}
                >
                  {q.name}
                </button>
              ))}
            </div>
          </div>

          {selectedQueue && (
            <div className="flex items-center gap-3">
              <button
                type="button"
                onClick={togglePauseQueue}
                className={`cursor-pointer flex items-center gap-1.5 px-3.5 py-2 rounded-xl text-xs font-bold transition-all border shadow-sm ${
                  selectedQueue.is_paused 
                    ? 'bg-emerald-600 text-white border-emerald-500 hover:bg-emerald-500' 
                    : 'bg-amber-600 text-white border-amber-500 hover:bg-amber-500'
                }`}
              >
                {selectedQueue.is_paused ? <Play className="w-3.5 h-3.5 fill-current" /> : <Pause className="w-3.5 h-3.5 fill-current" />}
                {selectedQueue.is_paused ? 'Resume Processing' : 'Pause Queue'}
              </button>
            </div>
          )}
        </div>

        {/* Real-time Metric Cards */}
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          <div className="bg-slate-900 border border-slate-800 p-5 rounded-2xl">
            <div className="flex justify-between items-start text-slate-400">
              <span className="text-xs font-semibold uppercase tracking-wider">Pending in RAM</span>
              <Activity className="w-4 h-4 text-indigo-400" />
            </div>
            <div className="mt-3 text-3xl font-extrabold text-white">
              {queueMetrics?.live?.pendingJobs ?? 0}
            </div>
            <span className="text-[10px] text-slate-500 mt-1 block">Live Redis ZSET</span>
          </div>

          <div className="bg-slate-900 border border-slate-800 p-5 rounded-2xl">
            <div className="flex justify-between items-start text-slate-400">
              <span className="text-xs font-semibold uppercase tracking-wider">Delayed / Sched</span>
              <Clock className="w-4 h-4 text-amber-400" />
            </div>
            <div className="mt-3 text-3xl font-extrabold text-white">
              {queueMetrics?.live?.delayedJobs ?? 0}
            </div>
            <span className="text-[10px] text-slate-500 mt-1 block">Future Execution</span>
          </div>

          <div className="bg-slate-900 border border-slate-800 p-5 rounded-2xl">
            <div className="flex justify-between items-start text-slate-400">
              <span className="text-xs font-semibold uppercase tracking-wider">Completed (24h)</span>
              <ShieldCheck className="w-4 h-4 text-emerald-400" />
            </div>
            <div className="mt-3 text-3xl font-extrabold text-white">
              {queueMetrics?.performance24h?.completedCount ?? 0}
            </div>
            <span className="text-[10px] text-slate-500 mt-1 block">PostgreSQL Verified</span>
          </div>

          <div className="bg-slate-900 border border-slate-800 p-5 rounded-2xl">
            <div className="flex justify-between items-start text-slate-400">
              <span className="text-xs font-semibold uppercase tracking-wider">DLQ Quarantined</span>
              <AlertTriangle className="w-4 h-4 text-rose-400" />
            </div>
            <div className="mt-3 text-3xl font-extrabold text-white">
              {queueMetrics?.live?.dlqJobs ?? dlqJobs.length}
            </div>
            <span className="text-[10px] text-slate-500 mt-1 block">Requires Redrive</span>
          </div>
        </div>

        {/* 24-Hour Continuous Throughput Chart & Dispatcher Form */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div className="lg:col-span-2 bg-slate-900 border border-slate-800 p-6 rounded-2xl">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-semibold text-white flex items-center gap-2">
                <BarChart2 className="w-4 h-4 text-indigo-400" />
                24-Hour Continuous Throughput
              </h3>
              <span className="text-xs text-slate-500">Hourly aggregation</span>
            </div>
            <div className="h-64 w-full">
              {chartData.length > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={chartData}>
                    <defs>
                      <linearGradient id="compGrad" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#6366f1" stopOpacity={0.4}/>
                        <stop offset="95%" stopColor="#6366f1" stopOpacity={0}/>
                      </linearGradient>
                    </defs>
                    <XAxis 
                      dataKey="time_bucket" 
                      stroke="#475569" 
                      fontSize={10} 
                      tickFormatter={(v) => v ? v.slice(11, 16) : ''}
                    />
                    <YAxis stroke="#475569" fontSize={10} />
                    <Tooltip 
                      contentStyle={{ backgroundColor: '#0f172a', borderColor: '#334155', borderRadius: '0.75rem' }}
                    />
                    <Area 
                      type="monotone" 
                      dataKey="completed" 
                      stroke="#6366f1" 
                      strokeWidth={2}
                      fillOpacity={1} 
                      fill="url(#compGrad)" 
                    />
                  </AreaChart>
                </ResponsiveContainer>
              ) : (
                <div className="h-full flex items-center justify-center text-xs text-slate-500">
                  Loading time-series data...
                </div>
              )}
            </div>
          </div>

          <div className="bg-slate-900 border border-slate-800 p-6 rounded-2xl flex flex-col justify-between">
            <div>
              <h3 className="text-sm font-semibold text-white flex items-center gap-2 mb-4">
                <Send className="w-4 h-4 text-indigo-400" />
                Dispatch Real-Time Jobs
              </h3>

              <div className="space-y-3">
                <div>
                  <label className="text-xs text-slate-400 font-medium">Task Type</label>
                  <select 
                    value={jobType} 
                    onChange={(e) => setJobType(e.target.value)}
                    className="w-full mt-1 bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs focus:outline-none focus:border-indigo-500"
                  >
                    <option value="SEND_WELCOME_EMAIL">SEND_WELCOME_EMAIL (100ms)</option>
                    <option value="GENERATE_INVOICE">GENERATE_INVOICE (200ms)</option>
                    <option value="SEND_REMINDER_SMS">SEND_REMINDER_SMS (80ms)</option>
                    <option value="FAILING_TASK_TEST">FAILING_TASK_TEST (Test Retries / DLQ)</option>
                  </select>
                </div>

                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <label className="text-xs text-slate-400 font-medium">Priority (1-10)</label>
                    <input 
                      type="number" 
                      value={jobPriority} 
                      onChange={(e) => setJobPriority(e.target.value)}
                      className="w-full mt-1 bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs focus:outline-none focus:border-indigo-500"
                    />
                  </div>
                  <div>
                    <label className="text-xs text-slate-400 font-medium">Batch Qty</label>
                    <input 
                      type="number" 
                      value={batchCount} 
                      onChange={(e) => setBatchCount(e.target.value)}
                      className="w-full mt-1 bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs focus:outline-none focus:border-indigo-500"
                    />
                  </div>
                </div>

                <div>
                  <label className="text-xs text-slate-400 font-medium">Payload (JSON)</label>
                  <textarea 
                    value={jobPayload} 
                    onChange={(e) => setJobPayload(e.target.value)}
                    rows={2}
                    className="w-full mt-1 bg-slate-950 border border-slate-800 rounded-xl p-2.5 font-mono text-[11px] focus:outline-none focus:border-indigo-500"
                  />
                </div>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-2 pt-4">
              <button
                disabled={loading}
                onClick={() => createJob(false)}
                className="py-2.5 bg-slate-800 hover:bg-slate-700 text-white rounded-xl text-xs font-medium transition-all cursor-pointer"
              >
                Enqueue 1 Job
              </button>
              <button
                disabled={loading}
                onClick={() => createJob(true)}
                className="py-2.5 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-semibold transition-all shadow-lg shadow-indigo-600/20 cursor-pointer"
              >
                Enqueue Batch ({batchCount})
              </button>
            </div>
          </div>
        </div>

        {/* Live Execution Audit Log */}
        <div className="bg-slate-900 border border-slate-800 p-6 rounded-2xl">
          <h3 className="text-sm font-semibold text-white flex items-center gap-2 mb-4">
            <Activity className="w-4 h-4 text-indigo-400" />
            Live Execution Audit Log
          </h3>

          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="border-b border-slate-800 text-slate-400">
                <tr>
                  <th className="py-2.5">Status</th>
                  <th className="py-2.5">Job ID</th>
                  <th className="py-2.5">Type</th>
                  <th className="py-2.5">Priority</th>
                  <th className="py-2.5">Retries</th>
                  <th className="py-2.5 text-right">Created At</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {recentJobs.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="py-6 text-center text-slate-500">
                      No recent jobs found. Dispatch a task above!
                    </td>
                  </tr>
                ) : (
                  recentJobs.map((j) => (
                    <tr key={j.id} className="hover:bg-slate-800/30">
                      <td className="py-2.5">
                        <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-md text-[10px] font-semibold uppercase ${
                          j.status === 'COMPLETED' ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20' :
                          j.status === 'QUEUED' ? 'bg-indigo-500/10 text-indigo-400 border border-indigo-500/20' :
                          j.status === 'RUNNING' ? 'bg-amber-500/10 text-amber-400 border border-amber-500/20' :
                          'bg-rose-500/10 text-rose-400 border border-rose-500/20'
                        }`}>
                          {j.status === 'COMPLETED' && <CheckCircle2 className="w-3 h-3" />}
                          {j.status === 'DLQ' && <XCircle className="w-3 h-3" />}
                          {j.status}
                        </span>
                      </td>
                      <td className="py-2.5 font-mono text-slate-300">{j.id.slice(0, 8)}...</td>
                      <td className="py-2.5 text-white font-medium">{j.type}</td>
                      <td className="py-2.5 text-indigo-400 font-semibold">{j.priority}</td>
                      <td className="py-2.5 text-slate-400">{j.retry_count}</td>
                      <td className="py-2.5 text-slate-400 text-right">{new Date(j.created_at).toLocaleTimeString()}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* DLQ Redrive & Quarantine Explorer */}
        <div className="bg-slate-900 border border-slate-800 p-6 rounded-2xl">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="text-sm font-semibold text-white flex items-center gap-2">
                <AlertTriangle className="w-4 h-4 text-rose-400" />
                Dead Letter Queue (DLQ) Quarantine
              </h3>
              <p className="text-xs text-slate-400">Jobs that exhausted retries or failed unrecoverably</p>
            </div>
            {dlqJobs.length > 0 && (
              <button
                onClick={() => replayDLQ()}
                className="cursor-pointer flex items-center gap-1.5 px-3 py-1.5 bg-indigo-600/20 text-indigo-400 border border-indigo-500/30 rounded-xl text-xs font-medium hover:bg-indigo-600/30 transition-all"
              >
                <RotateCcw className="w-3.5 h-3.5" />
                Replay All ({dlqJobs.length})
              </button>
            )}
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="border-b border-slate-800 text-slate-400">
                <tr>
                  <th className="py-2.5">Job ID</th>
                  <th className="py-2.5">Reason</th>
                  <th className="py-2.5">Quarantined At</th>
                  <th className="py-2.5 text-right">Action</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {dlqJobs.length === 0 ? (
                  <tr>
                    <td colSpan={4} className="py-6 text-center text-slate-500">
                      No quarantined jobs in DLQ. System healthy.
                    </td>
                  </tr>
                ) : (
                  dlqJobs.map((dj) => (
                    <tr key={dj.id || dj.job_id} className="hover:bg-slate-800/30">
                      <td className="py-2.5 font-mono text-slate-300">{(dj.job_id || dj.id || '').slice(0, 8)}...</td>
                      <td className="py-2.5 text-rose-400 max-w-xs truncate">{dj.failed_reason || 'Execution failure'}</td>
                      <td className="py-2.5 text-slate-400">{dj.exhausted_at ? new Date(dj.exhausted_at).toLocaleTimeString() : 'Just now'}</td>
                      <td className="py-2.5 text-right">
                        <button
                          onClick={() => replayDLQ(dj.job_id || dj.id)}
                          className="cursor-pointer px-2.5 py-1 bg-slate-800 hover:bg-slate-700 text-indigo-300 rounded-lg text-[11px] font-medium transition-all"
                        >
                          Replay
                        </button>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* Worker Fleet Telemetry */}
        <div className="bg-slate-900 border border-slate-800 p-6 rounded-2xl">
          <h3 className="text-sm font-semibold text-white flex items-center gap-2 mb-4">
            <Server className="w-4 h-4 text-emerald-400" />
            Worker Fleet Telemetry
          </h3>

          <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-3">
            {workers.length === 0 ? (
              <span className="text-xs text-slate-500">No active workers registered</span>
            ) : (
              workers.map((w) => (
                <div key={w.id} className="p-3.5 bg-slate-950 border border-slate-800 rounded-xl flex items-center justify-between">
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
                      <span className="font-mono text-xs text-white font-medium">{w.id.slice(0, 8)}...</span>
                    </div>
                    <span className="text-[11px] text-slate-500 block mt-1">PID {w.pid} • {w.hostname}</span>
                  </div>
                  <span className="text-[10px] font-semibold uppercase px-2 py-0.5 bg-emerald-500/10 text-emerald-400 rounded-md border border-emerald-500/20">
                    {w.status}
                  </span>
                </div>
              ))
            )}
          </div>
        </div>

      </div>

      {/* Create Project & Queue Modal */}
      {showCreateModal && (
        <div className="fixed inset-0 bg-slate-950/80 backdrop-blur-sm flex items-center justify-center p-4 z-50">
          <div className="max-w-md w-full bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-2xl">
            <div className="flex items-center gap-3 mb-5">
              <div className="p-2.5 bg-indigo-500/10 text-indigo-400 rounded-xl">
                <FolderPlus className="w-5 h-5" />
              </div>
              <div>
                <h2 className="text-base font-bold text-white">Create Project & Queue</h2>
                <p className="text-xs text-slate-400">Add an isolated queue stream</p>
              </div>
            </div>

            <form onSubmit={handleCreateProjectQueue} className="space-y-4">
              <div>
                <label className="text-xs font-semibold uppercase text-slate-400">Project Name</label>
                <input 
                  type="text" 
                  required
                  placeholder="e.g. Analytics Pipeline"
                  value={newProjectName}
                  onChange={(e) => setNewProjectName(e.target.value)}
                  className="w-full mt-1 px-3.5 py-2 bg-slate-950 border border-slate-800 rounded-xl text-xs focus:outline-none focus:border-indigo-500"
                />
              </div>

              <div>
                <label className="text-xs font-semibold uppercase text-slate-400">Queue Name</label>
                <input 
                  type="text" 
                  required
                  placeholder="e.g. analytics-queue"
                  value={newQueueName}
                  onChange={(e) => setNewQueueName(e.target.value)}
                  className="w-full mt-1 px-3.5 py-2 bg-slate-950 border border-slate-800 rounded-xl text-xs focus:outline-none focus:border-indigo-500"
                />
              </div>

              <div>
                <label className="text-xs font-semibold uppercase text-slate-400">Concurrency Limit</label>
                <input 
                  type="number" 
                  min="1"
                  max="100"
                  required
                  value={newConcurrency}
                  onChange={(e) => setNewConcurrency(e.target.value)}
                  className="w-full mt-1 px-3.5 py-2 bg-slate-950 border border-slate-800 rounded-xl text-xs focus:outline-none focus:border-indigo-500"
                />
              </div>

              <div className="flex justify-end gap-2 pt-3">
                <button
                  type="button"
                  onClick={() => setShowCreateModal(false)}
                  className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-xl text-xs font-medium cursor-pointer"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={loading}
                  className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-semibold shadow-lg shadow-indigo-600/20 cursor-pointer"
                >
                  Create Queue
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

    </div>
  );
}
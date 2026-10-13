import { useEffect, useState } from 'react';
import type { Department, PropagationEvent } from '../lib/types';
import { getDashboardStats } from '../lib/api';
import {
  CheckCircle2, AlertTriangle, RefreshCw, Clock,
  ArrowRight, TrendingUp, Database, Zap, GitMerge, Activity,
  ArrowUpRight, ArrowDownLeft
} from 'lucide-react';

type Stats = {
  totalEvents: number;
  successCount: number;
  failureCount: number;
  conflictCount: number;
  duplicateCount: number;
  pendingCount: number;
  dlqCount: number;
  activeConflicts: number;
  pendingMappings: number;
  totalBusinesses: number;
  activeDepts: number;
  avgPropMs: number;
  swsToDept: number;
  deptToSws: number;
};

const OUTCOME_CFG = {
  success: { color: 'text-success', dot: '#15be53' },
  failure: { color: 'text-danger', dot: '#EF4444' },
  conflict: { color: 'text-warning', dot: '#F59E0B' },
  duplicate: { color: 'text-amber-500', dot: '#f59e0b' },
  pending: { color: 'text-primary', dot: '#533afd' },
  dlq: { color: 'text-danger', dot: '#dc2626' },
  held: { color: 'text-slate-500', dot: '#64748b' },
};

function MiniBar({ label, count, total, color }: { label: string; count: number; total: number; color: string }) {
  const pct = total > 0 ? (count / total) * 100 : 0;
  return (
    <div>
      <div className="flex justify-between items-center mb-1.5">
        <span className="text-[12px] text-body capitalize font-medium">{label}</span>
        <span className="text-[12px] font-semibold tabular-nums" style={{ color }}>{count}</span>
      </div>
      <div className="h-1.5 bg-gray-100 rounded-full overflow-hidden">
        <div className="h-full rounded-full transition-all duration-700" style={{ width: `${pct}%`, background: color }} />
      </div>
    </div>
  );
}

function StatCard({ label, value, sub, icon: Icon, color, bg }: {
  label: string; value: string | number; sub?: string;
  icon: typeof CheckCircle2; color: string; bg: string;
}) {
  return (
    <div className="card">
      <div className="flex items-start justify-between p-5">
        <div>
          <p className="text-[12px] text-body font-medium mb-1">{label}</p>
          <p className="text-[28px] font-semibold text-navy tracking-tight">{value}</p>
          {sub && <p className="text-[11px] text-muted mt-1">{sub}</p>}
        </div>
        <div className={`p-2.5 rounded-md ${bg}`}>
          <Icon size={20} className={color} />
        </div>
      </div>
    </div>
  );
}

const DEPT_STATUS_DOT: Record<Department['status'], string> = {
  active: 'bg-success',
  degraded: 'bg-warning',
  offline: 'bg-danger',
  circuit_open: 'bg-danger',
};

const TIER_LABELS = ['', 'T1 Webhook', 'T2 Poll', 'T3 Snap'];

const PIPELINE_NODES = [
  { label: 'SWS / Dept Systems', sub: 'Event source', border: 'border-primary/30', bg: 'bg-primary/5', text: 'text-primary' },
  { label: 'Ingestion Engine', sub: 'Hook · Poll · Snapshot', border: 'border-gray-300', bg: 'bg-gray-50', text: 'text-navy' },
  { label: 'Kafka Queue', sub: 'At-least-once', border: 'border-warning/30', bg: 'bg-warning/5', text: 'text-warning' },
  { label: 'Transform Engine', sub: 'AI schema mapping', border: 'border-cyan-500/30', bg: 'bg-cyan-500/5', text: 'text-cyan-600' },
  { label: 'Conflict Resolver', sub: 'SWS-wins · LWW · Manual', border: 'border-orange-500/30', bg: 'bg-orange-500/5', text: 'text-orange-600' },
  { label: 'Destination', sub: 'Idempotent write', border: 'border-success/30', bg: 'bg-success/5', text: 'text-success' },
];

export default function Dashboard() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [recentEvents, setRecentEvents] = useState<PropagationEvent[]>([]);
  const [departments, setDepartments] = useState<Department[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      const data = await getDashboardStats();
      const events = data.events || [];
      const ms = events.filter(e => e.propagation_ms != null).map(e => e.propagation_ms as number);
      const avg = ms.length > 0 ? Math.round(ms.reduce((a, b) => a + b, 0) / ms.length) : 0;
      const cnt = (o: string) => events.filter(e => e.outcome === o).length;

      setStats({
        totalEvents: data.totalEvents || events.length,
        successCount: cnt('success'),
        failureCount: cnt('failure'),
        conflictCount: cnt('conflict'),
        duplicateCount: cnt('duplicate'),
        pendingCount: cnt('pending'),
        dlqCount: cnt('dlq'),
        activeConflicts: data.activeConflicts,
        pendingMappings: data.pendingMappings,
        totalBusinesses: data.totalBusinesses,
        activeDepts: data.activeDepts,
        avgPropMs: data.avgPropMs || avg,
        swsToDept: data.swsToDept,
        deptToSws: data.deptToSws,
      });
      setDepartments(data.departments || []);
      setRecentEvents(data.recentEvents || []);
      setLoading(false);
    }
    load();
  }, []);

  if (loading) return (
    <div className="flex items-center justify-center h-full min-h-[300px]">
      <div className="flex flex-col items-center gap-3">
        <div className="w-7 h-7 rounded-full border-2 border-primary border-t-transparent animate-spin" />
        <span className="text-sm text-body">Loading dashboard data...</span>
      </div>
    </div>
  );

  const s = stats!;
  const successRate = s.totalEvents > 0 ? ((s.successCount / s.totalEvents) * 100).toFixed(1) : '0.0';

  return (
    <div className="space-y-6 animate-fade-in">
      {/* Top stats */}
      <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4">
        <StatCard label="Total Events" value={s.totalEvents.toLocaleString()} sub="all propagations" icon={GitMerge} color="text-primary" bg="bg-primary/10" />
        <StatCard label="Success Rate" value={`${successRate}%`} sub={`${s.successCount} succeeded`} icon={TrendingUp} color="text-success" bg="bg-success/10" />
        <StatCard label="Active Conflicts" value={s.activeConflicts} sub="needs resolution" icon={AlertTriangle} color="text-warning" bg="bg-warning/10" />
        <StatCard label="Pending Mappings" value={s.pendingMappings} sub="awaiting review" icon={Clock} color="text-amber-500" bg="bg-amber-500/10" />
        <StatCard label="Avg Latency" value={s.avgPropMs > 0 ? `${s.avgPropMs}ms` : '—'} sub="propagation time" icon={Activity} color="text-cyan-500" bg="bg-cyan-500/10" />
        <StatCard label="Dept Systems" value={`${s.activeDepts}/${departments.length}`} sub="online" icon={Database} color="text-gray-600" bg="bg-gray-100" />
      </div>

      {/* Direction split */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="card p-5 flex items-center gap-4">
          <div className="p-3 bg-primary/10 rounded-lg flex-shrink-0">
            <ArrowUpRight size={20} className="text-primary" />
          </div>
          <div className="flex-1 min-w-0">
            <p className="text-[12px] text-body font-medium uppercase tracking-wider">SWS → Dept</p>
            <p className="text-[28px] font-bold text-navy tabular-nums mt-0.5">{s.swsToDept.toLocaleString()}</p>
            <p className="text-[12px] text-body mt-0.5">Broadcast from central SWS to department systems</p>
          </div>
          <div className="flex-shrink-0 text-right">
            <p className="text-sm font-semibold text-navy">
              {s.totalEvents > 0 ? ((s.swsToDept / s.totalEvents) * 100).toFixed(0) : 0}%
            </p>
            <p className="text-[11px] text-muted">of total</p>
          </div>
        </div>
        <div className="card p-5 flex items-center gap-4">
          <div className="p-3 bg-cyan-500/10 rounded-lg flex-shrink-0">
            <ArrowDownLeft size={20} className="text-cyan-600" />
          </div>
          <div className="flex-1 min-w-0">
            <p className="text-[12px] text-body font-medium uppercase tracking-wider">Dept → SWS</p>
            <p className="text-[28px] font-bold text-navy tabular-nums mt-0.5">{s.deptToSws.toLocaleString()}</p>
            <p className="text-[12px] text-body mt-0.5">Updates ingested from department systems into SWS</p>
          </div>
          <div className="flex-shrink-0 text-right">
            <p className="text-sm font-semibold text-navy">
              {s.totalEvents > 0 ? ((s.deptToSws / s.totalEvents) * 100).toFixed(0) : 0}%
            </p>
            <p className="text-[11px] text-muted">of total</p>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        {/* Outcome breakdown */}
        <div className="card p-6">
          <h2 className="font-semibold text-navy text-[15px] mb-5">Outcome Breakdown</h2>
          <div className="space-y-4">
            <MiniBar label="Success" count={s.successCount} total={s.totalEvents} color={OUTCOME_CFG.success.dot} />
            <MiniBar label="Failure" count={s.failureCount} total={s.totalEvents} color={OUTCOME_CFG.failure.dot} />
            <MiniBar label="Conflict" count={s.conflictCount} total={s.totalEvents} color={OUTCOME_CFG.conflict.dot} />
            <MiniBar label="Duplicate (idempotent skip)" count={s.duplicateCount} total={s.totalEvents} color={OUTCOME_CFG.duplicate.dot} />
            <MiniBar label="Pending / In-flight" count={s.pendingCount} total={s.totalEvents} color={OUTCOME_CFG.pending.dot} />
            <MiniBar label="Dead Letter Queue" count={s.dlqCount} total={s.totalEvents} color={OUTCOME_CFG.dlq.dot} />
          </div>
          <div className="mt-5 pt-5 border-t border-[#e5edf5] grid grid-cols-2 gap-4">
            <div className="bg-gray-50 rounded-lg p-4 text-center border border-[#e5edf5]">
              <p className="text-[11px] text-body font-medium uppercase tracking-wide">Businesses</p>
              <p className="text-xl font-bold text-navy mt-1">{s.totalBusinesses}</p>
            </div>
            <div className="bg-danger/5 rounded-lg p-4 text-center border border-danger/10">
              <p className="text-[11px] text-danger font-medium uppercase tracking-wide">DLQ Events</p>
              <p className="text-xl font-bold text-danger mt-1">{s.dlqCount}</p>
            </div>
          </div>
        </div>

        {/* Recent events */}
        <div className="xl:col-span-2 card flex flex-col h-full">
          <div className="px-6 py-4 border-b border-[#e5edf5] flex items-center justify-between">
            <h2 className="font-semibold text-navy text-[15px]">Recent Propagation Events</h2>
            <span className="text-[12px] text-body">Last 12</span>
          </div>
          <div className="divide-y divide-[#e5edf5] flex-1">
            {recentEvents.length === 0 ? (
              <div className="p-8 text-center text-body text-sm h-full flex items-center justify-center">
                No events yet — start the simulator or click "Inject 8 Events" in the sidebar.
              </div>
            ) : recentEvents.map(ev => {
              const cfg = OUTCOME_CFG[ev.outcome as keyof typeof OUTCOME_CFG] ?? OUTCOME_CFG.pending;
              return (
                <div key={ev.id} className="px-6 py-3 flex items-center gap-4 hover:bg-gray-50 transition-colors">
                  <span className="w-2 h-2 rounded-full flex-shrink-0 shadow-sm" style={{ background: cfg.dot }} />
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 min-w-0">
                      <span className="text-[13px] font-mono text-navy font-medium truncate">{ev.ubid}</span>
                      <span className="text-[11px] text-muted">·</span>
                      <span className="text-[12px] text-body capitalize whitespace-nowrap">{ev.event_type.replace(/_/g, ' ')}</span>
                    </div>
                    <div className="flex items-center gap-1.5 mt-1">
                      <span className="text-[11px] font-medium text-body bg-gray-100 px-1.5 py-0.5 rounded">{ev.source_system}</span>
                      <ArrowRight size={10} className="text-muted" />
                      <span className="text-[11px] font-medium text-body bg-gray-100 px-1.5 py-0.5 rounded">{ev.destination_system}</span>
                      {ev.propagation_ms && <span className="text-[11px] text-muted ml-2">{ev.propagation_ms}ms</span>}
                    </div>
                  </div>
                  <div className="flex-shrink-0 text-right">
                    <span className={`inline-flex items-center px-2 py-1 rounded text-[11px] font-semibold capitalize bg-gray-50 border border-gray-100 ${cfg.color}`}>{ev.outcome}</span>
                    {ev.retry_count > 0 && <p className="text-[11px] text-muted mt-1">{ev.retry_count}x retry</p>}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Department health */}
      <div className="card">
        <div className="px-6 py-4 border-b border-[#e5edf5]">
          <h2 className="font-semibold text-navy text-[15px]">Department System Health</h2>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8 divide-x divide-y sm:divide-y-0 divide-[#e5edf5]">
          {departments.map(dept => (
            <div key={dept.id} className="px-4 py-4 text-center hover:bg-gray-50 transition-colors">
              <div className="flex items-center justify-center gap-2 mb-2">
                <div className={`w-2 h-2 rounded-full ${DEPT_STATUS_DOT[dept.status]} ${dept.status === 'active' ? 'animate-ping-slow' : ''}`} />
                <span className="text-[13px] font-bold text-navy">{dept.code}</span>
              </div>
              <p className="text-[11px] font-medium text-body">{TIER_LABELS[dept.ingestion_tier]}</p>
              <p className="text-[11px] text-muted mt-1">{dept.records_synced.toLocaleString()} recs</p>
            </div>
          ))}
        </div>
      </div>

      {/* Pipeline */}
      <div className="card p-6">
        <h2 className="font-semibold text-navy text-[15px] mb-5">Live Data Pipeline</h2>
        <div className="flex items-center gap-3 overflow-x-auto pb-2 scrollbar-hide">
          {PIPELINE_NODES.map((node, i) => (
            <div key={node.label} className="flex items-center gap-3 flex-shrink-0">
              <div className={`border rounded-xl px-4 py-3 text-center min-w-[140px] shadow-sm ${node.border} ${node.bg}`}>
                <p className={`text-[12px] font-semibold ${node.text}`}>{node.label}</p>
                <p className="text-[11px] text-body mt-1">{node.sub}</p>
              </div>
              {i < PIPELINE_NODES.length - 1 && <ArrowRight size={16} className="text-gray-300 flex-shrink-0" />}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

import { useEffect, useState, useCallback } from 'react';
import type { PropagationEvent } from '../lib/types';
import { listPropagationEvents, replayPropagationEvent } from '../lib/api';
import {
  CheckCircle2, XCircle, AlertTriangle, RefreshCw, Clock,
  ArrowRight, Search, Info, ChevronDown, ChevronUp, RotateCcw,
  ArrowUpRight, ArrowDownLeft
} from 'lucide-react';

const OUTCOME_CFG: Record<string, { label: string; color: string; bg: string; border: string; dot: string }> = {
  success: { label: 'Success', color: 'text-success', bg: 'bg-success/10', border: 'border-success/20', dot: '#15be53' },
  failure: { label: 'Failure', color: 'text-danger', bg: 'bg-danger/10', border: 'border-danger/20', dot: '#EF4444' },
  conflict: { label: 'Conflict', color: 'text-warning', bg: 'bg-warning/10', border: 'border-warning/20', dot: '#F59E0B' },
  duplicate: { label: 'Duplicate', color: 'text-amber-500', bg: 'bg-amber-500/10', border: 'border-amber-500/20', dot: '#f59e0b' },
  pending: { label: 'Pending', color: 'text-primary', bg: 'bg-primary/10', border: 'border-primary/20', dot: '#533afd' },
  dlq: { label: 'DLQ', color: 'text-danger', bg: 'bg-danger/10', border: 'border-danger/20', dot: '#dc2626' },
  held: { label: 'Held', color: 'text-slate-500', bg: 'bg-slate-500/10', border: 'border-slate-500/20', dot: '#64748b' },
};

function OutcomeIcon({ outcome }: { outcome: string }) {
  if (outcome === 'success') return <CheckCircle2 size={13} className="text-success" />;
  if (outcome === 'failure' || outcome === 'dlq') return <XCircle size={13} className="text-danger" />;
  if (outcome === 'conflict') return <AlertTriangle size={13} className="text-warning" />;
  if (outcome === 'duplicate') return <RefreshCw size={13} className="text-amber-500" />;
  return <Clock size={13} className="text-primary" />;
}

function DirectionBadge({ direction }: { direction: PropagationEvent['direction'] }) {
  if (direction === 'sws_to_dept') {
    return (
      <span className="inline-flex items-center gap-1 text-[10px] font-semibold text-primary bg-primary/10 border border-primary/20 px-1.5 py-0.5 rounded whitespace-nowrap">
        <ArrowUpRight size={10} /> SWS→Dept
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 text-[10px] font-semibold text-cyan-600 bg-cyan-600/10 border border-cyan-600/20 px-1.5 py-0.5 rounded whitespace-nowrap">
      <ArrowDownLeft size={10} /> Dept→SWS
    </span>
  );
}

const BACKOFF_SCHEDULE = ['5s', '30s', '2m', '10m', 'DLQ'];
const PAGE_SIZE = 25;

function EventRow({ event, expanded, onToggle, onReplay }: {
  event: PropagationEvent;
  expanded: boolean;
  onToggle: () => void;
  onReplay: (event: PropagationEvent) => void;
}) {
  const cfg = OUTCOME_CFG[event.outcome] ?? OUTCOME_CFG.pending;
  const time = new Date(event.created_at).toLocaleTimeString('en-IN', { hour12: false });

  return (
    <>
      <button
        onClick={onToggle}
        className={`w-full grid items-center text-left transition-colors ${expanded ? 'bg-gray-50' : 'bg-white hover:bg-gray-50/50'}`}
        style={{ gridTemplateColumns: '32px 1fr 100px 110px 70px 50px 70px 28px' }}
      >
        <div className="h-full flex items-center justify-center py-3.5 border-l-2" style={{ borderLeftColor: cfg.dot }}>
          <OutcomeIcon outcome={event.outcome} />
        </div>
        <div className="py-3.5 pr-4 min-w-0">
          <div className="flex items-center gap-2 min-w-0">
            <span className="text-[13px] font-mono text-navy font-medium truncate">{event.ubid}</span>
            <span className={`text-[10px] px-2 py-0.5 rounded border font-semibold flex-shrink-0 ${cfg.bg} ${cfg.border} ${cfg.color}`}>
              {event.event_type.replace(/_/g, ' ')}
            </span>
            <DirectionBadge direction={event.direction} />
          </div>
        </div>
        <div className="py-3.5">
          <span className="text-[11px] text-navy font-medium bg-gray-100 border border-gray-200 px-2.5 py-1 rounded-md">{event.source_system}</span>
        </div>
        <div className="py-3.5 flex items-center gap-1.5">
          <ArrowRight size={10} className="text-muted flex-shrink-0" />
          <span className="text-[11px] text-navy font-medium bg-gray-100 border border-gray-200 px-2.5 py-1 rounded-md">{event.destination_system}</span>
        </div>
        <div className="py-3.5 text-right pr-3">
          <span className="text-[12px] text-body tabular-nums font-medium">
            {event.propagation_ms != null ? `${event.propagation_ms}ms` : '—'}
          </span>
        </div>
        <div className="py-3.5 text-center">
          <span className={`text-[12px] tabular-nums font-semibold ${event.retry_count > 0 ? 'text-warning bg-warning/10 px-1.5 py-0.5 rounded' : 'text-muted'}`}>
            {event.retry_count}
          </span>
        </div>
        <div className="py-3.5 pr-2 text-right">
          <span className="text-[11px] text-muted tabular-nums font-medium">{time}</span>
        </div>
        <div className="py-3.5 flex items-center justify-center">
          {expanded ? <ChevronUp size={14} className="text-body" /> : <ChevronDown size={14} className="text-muted" />}
        </div>
      </button>

      {expanded && (
        <div className="bg-gray-50/50 border-t border-b border-[#e5edf5] px-8 py-5">
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-5">
            <div>
              <p className="text-[11px] text-body font-medium uppercase tracking-wider mb-1.5">Outcome</p>
              <span className={`text-[13px] font-bold capitalize ${cfg.color}`}>{event.outcome}</span>
            </div>
            <div>
              <p className="text-[11px] text-body font-medium uppercase tracking-wider mb-1.5">Direction</p>
              <DirectionBadge direction={event.direction} />
            </div>
            <div>
              <p className="text-[11px] text-body font-medium uppercase tracking-wider mb-1.5">Conflict Flag</p>
              <span className={`text-[13px] font-semibold ${event.conflict_flag ? 'text-warning' : 'text-body'}`}>
                {event.conflict_flag ? 'Yes — conflict detected' : 'No'}
              </span>
            </div>
            {event.resolution_applied && (
              <div>
                <p className="text-[11px] text-body font-medium uppercase tracking-wider mb-1.5">Resolution</p>
                <span className="text-[13px] text-navy font-semibold capitalize">{event.resolution_applied.replace(/_/g, ' ')}</span>
              </div>
            )}
            <div>
              <p className="text-[11px] text-body font-medium uppercase tracking-wider mb-1.5">Retries</p>
              <span className={`text-[13px] font-semibold ${event.retry_count > 0 ? 'text-warning' : 'text-body'}`}>
                {event.retry_count === 0 ? 'None' : `${event.retry_count} retr${event.retry_count > 1 ? 'ies' : 'y'}`}
              </span>
            </div>
            <div className="col-span-2">
              <p className="text-[11px] text-body font-medium uppercase tracking-wider mb-2">Exponential Backoff Schedule</p>
              <div className="flex items-center gap-2 flex-wrap">
                {BACKOFF_SCHEDULE.map((step, i) => {
                  const isDlq = step === 'DLQ';
                  const reached = isDlq ? event.outcome === 'dlq' : i < event.retry_count;
                  const current = !isDlq && i === event.retry_count - 1 && event.outcome !== 'dlq';
                  return (
                    <span key={step} className={`text-[11px] px-2.5 py-1 rounded-md border font-mono font-bold shadow-sm ${
                      isDlq && event.outcome === 'dlq'
                        ? 'bg-danger/10 border-danger/20 text-danger'
                        : current
                          ? 'bg-warning/10 border-warning/20 text-warning'
                          : reached
                            ? 'bg-gray-200 border-gray-300 text-navy'
                            : 'bg-white border-gray-200 text-muted'
                    }`}>{step}</span>
                  );
                })}
              </div>
            </div>
            <div className="col-span-2">
              <p className="text-[11px] text-body font-medium uppercase tracking-wider mb-1.5">Idempotency Key</p>
              <code className="text-[12px] text-navy font-semibold font-mono break-all">{event.idempotency_key}</code>
            </div>
            {event.payload_hash && (
              <div className="col-span-2">
                <p className="text-[11px] text-body font-medium uppercase tracking-wider mb-1.5">Payload Hash</p>
                <code className="text-[12px] text-navy font-semibold font-mono">{event.payload_hash}</code>
              </div>
            )}
            {event.error_message && (
              <div className="col-span-4 bg-danger/5 border border-danger/20 rounded-md p-3">
                <p className="text-[11px] text-danger font-bold uppercase tracking-wider mb-1">Error</p>
                <p className="text-[13px] font-medium text-danger">{event.error_message}</p>
              </div>
            )}
            <div className="col-span-2">
              <p className="text-[11px] text-body font-medium uppercase tracking-wider mb-1.5">Timestamp</p>
              <p className="text-[13px] text-navy font-medium">{new Date(event.created_at).toLocaleString('en-IN')}</p>
            </div>
            {event.outcome === 'dlq' && (
              <div className="col-span-4 pt-4 mt-2 border-t border-[#e5edf5]">
                <p className="text-[11px] text-body font-bold uppercase tracking-wider mb-2">Dead Letter Queue — Replay</p>
                <div className="flex items-center gap-4">
                  <p className="text-[12px] text-body flex-1 leading-relaxed">
                    5 retries exhausted (5s → 30s → 2m → 10m → DLQ). Replay will re-queue this event as a new pending propagation with reset retry count.
                  </p>
                  <button
                    onClick={(e) => { e.stopPropagation(); onReplay(event); }}
                    className="flex items-center gap-1.5 px-4 py-2 bg-danger/10 hover:bg-danger/20 border border-danger/20 text-danger text-[13px] rounded-md transition-colors font-bold flex-shrink-0"
                  >
                    <RotateCcw size={14} /> Replay from DLQ
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </>
  );
}

export default function EventFeed() {
  const [events, setEvents] = useState<PropagationEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState<string>('all');
  const [search, setSearch] = useState('');
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(0);
  const [replayToast, setReplayToast] = useState<string | null>(null);

  const fetchEvents = useCallback(async () => {
    const { data, total } = await listPropagationEvents({
      limit: PAGE_SIZE,
      offset: page * PAGE_SIZE,
      outcome: filter,
      q: search.trim() || undefined,
    });
    setEvents(data || []);
    setTotal(total || 0);
    setLoading(false);
  }, [filter, search, page]);

  useEffect(() => { fetchEvents(); }, [fetchEvents]);

  useEffect(() => { setPage(0); }, [filter, search]);

  async function handleReplay(event: PropagationEvent) {
    await replayPropagationEvent(event);
    setReplayToast(`Replayed ${event.ubid} — queued as pending`);
    setTimeout(() => setReplayToast(null), 3500);
    await fetchEvents();
  }

  const counts = events.reduce((a, e) => { a[e.outcome] = (a[e.outcome] || 0) + 1; return a; }, {} as Record<string, number>);
  const totalAll = Object.values(counts).reduce((a, b) => a + b, 0);
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  return (
    <div className="space-y-4 h-[calc(100vh-140px)] flex flex-col animate-fade-in">
      {replayToast && (
        <div className="fixed top-6 right-6 z-50 flex items-center gap-2 bg-white border border-success text-success text-sm px-4 py-3 rounded-lg shadow-panel animate-slide-up font-medium">
          <RotateCcw size={16} /> {replayToast}
        </div>
      )}
      {/* Controls */}
      <div className="flex items-center gap-4 flex-wrap flex-shrink-0">
        <div className="relative flex-1 min-w-[280px]">
          <Search size={16} className="absolute left-3.5 top-1/2 -translate-y-1/2 text-muted" />
          <input
            type="text"
            placeholder="Search UBID, event type, system..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="w-full bg-white border border-[#e5edf5] rounded-md pl-10 pr-4 py-2 text-[13px] text-navy placeholder-muted focus:outline-none focus:border-primary focus:ring-1 focus:ring-primary shadow-sm transition-all"
          />
        </div>
        <div className="flex gap-2 flex-wrap">
          {(['all', ...Object.keys(OUTCOME_CFG)] as string[]).map(o => {
            const cfg = OUTCOME_CFG[o];
            const cnt = o === 'all' ? totalAll : (counts[o] || 0);
            return (
              <button
                key={o}
                onClick={() => setFilter(o)}
                className={`px-3 py-1.5 rounded-md text-[12px] font-semibold border transition-all capitalize shadow-sm ${
                  filter === o
                    ? o === 'all'
                      ? 'bg-navy text-white border-navy'
                      : `${cfg.bg} ${cfg.border} ${cfg.color}`
                    : 'bg-white text-body border-[#e5edf5] hover:border-gray-300 hover:text-navy'
                }`}
              >
                {o === 'all' ? 'All' : cfg.label} ({cnt})
              </button>
            );
          })}
        </div>
        <button
          onClick={fetchEvents}
          className="flex items-center gap-2 px-4 py-2 bg-white hover:bg-gray-50 border border-[#e5edf5] rounded-md text-[13px] text-navy transition-colors font-medium shadow-sm ml-auto"
        >
          <RefreshCw size={14} /> Refresh
        </button>
      </div>

      {/* Table */}
      <div className="card flex-1 flex flex-col overflow-hidden">
        <div
          className="grid text-[11px] text-body font-bold uppercase tracking-wider border-b border-[#e5edf5] bg-gray-50"
          style={{ gridTemplateColumns: '32px 1fr 100px 110px 70px 50px 70px 28px' }}
        >
          <div className="py-3.5 px-2" />
          <div className="py-3.5">Event · UBID</div>
          <div className="py-3.5">Source</div>
          <div className="py-3.5">Destination</div>
          <div className="py-3.5 text-right pr-3">Latency</div>
          <div className="py-3.5 text-center">Retries</div>
          <div className="py-3.5 text-right pr-2">Time</div>
          <div />
        </div>

        <div className="flex-1 overflow-y-auto divide-y divide-[#e5edf5] bg-white">
          {loading ? (
            <div className="flex items-center justify-center h-full min-h-[200px]">
              <RefreshCw size={24} className="animate-spin text-body" />
            </div>
          ) : events.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-full min-h-[200px] gap-3 text-body">
              <div className="p-4 bg-gray-50 rounded-full">
                <Info size={24} className="text-muted" />
              </div>
              <p className="text-[15px] font-medium text-navy">No events found</p>
              <p className="text-[13px]">Start the simulator from the sidebar or click "Inject 8 Events"</p>
            </div>
          ) : (
              events.map(event => (
              <EventRow
                key={event.id}
                event={event}
                expanded={expandedId === event.id}
                onToggle={() => setExpandedId(expandedId === event.id ? null : event.id)}
                onReplay={handleReplay}
              />
            ))
          )}
        </div>

        <div className="px-6 py-3 border-t border-[#e5edf5] bg-gray-50 flex items-center justify-between text-[12px] text-body font-medium">
          <span>Showing {events.length} of {total} events</span>
          <div className="flex items-center gap-2">
            <button disabled={page === 0} onClick={() => setPage(0)} className="px-2 py-1 rounded border border-[#e5edf5] bg-white hover:bg-gray-50 disabled:opacity-50 shadow-sm transition-colors">«</button>
            <button disabled={page === 0} onClick={() => setPage(p => Math.max(0, p - 1))} className="px-3 py-1 rounded border border-[#e5edf5] bg-white hover:bg-gray-50 disabled:opacity-50 shadow-sm transition-colors text-navy">Prev</button>
            <span className="text-navy px-2">Page {page + 1} of {totalPages}</span>
            <button disabled={page >= totalPages - 1} onClick={() => setPage(p => p + 1)} className="px-3 py-1 rounded border border-[#e5edf5] bg-white hover:bg-gray-50 disabled:opacity-50 shadow-sm transition-colors text-navy">Next</button>
            <button disabled={page >= totalPages - 1} onClick={() => setPage(totalPages - 1)} className="px-2 py-1 rounded border border-[#e5edf5] bg-white hover:bg-gray-50 disabled:opacity-50 shadow-sm transition-colors">»</button>
          </div>
        </div>
      </div>
    </div>
  );
}

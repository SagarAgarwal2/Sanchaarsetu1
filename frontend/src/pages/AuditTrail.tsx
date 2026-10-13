import { useEffect, useState } from 'react';
import type { AuditRecord } from '../lib/types';
import { listAuditTrail } from '../lib/api';
import { RefreshCw, Search, Lock, Download } from 'lucide-react';

const OUTCOME_CFG: Record<string, { color: string; bg: string; border: string }> = {
  success: { color: 'text-success', bg: 'bg-success/10', border: 'border-success/20' },
  failure: { color: 'text-danger', bg: 'bg-danger/10', border: 'border-danger/20' },
  conflict: { color: 'text-warning', bg: 'bg-warning/10', border: 'border-warning/20' },
  duplicate: { color: 'text-amber-500', bg: 'bg-amber-500/10', border: 'border-amber-500/20' },
  pending: { color: 'text-primary', bg: 'bg-primary/10', border: 'border-primary/20' },
  dlq: { color: 'text-danger', bg: 'bg-danger/10', border: 'border-danger/20' },
  held: { color: 'text-slate-500', bg: 'bg-slate-500/10', border: 'border-slate-500/20' },
};

const PER_PAGE = 25;

export default function AuditTrail() {
  const [events, setEvents] = useState<AuditRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [page, setPage] = useState(0);

  async function load() {
    setLoading(true);
    const data = await listAuditTrail({
      limit: PER_PAGE,
      offset: page * PER_PAGE,
    });
    setEvents(data || []);
    setLoading(false);
  }

  useEffect(() => { load(); }, [page]);

  const filtered = events.filter(e =>
    search === '' ||
    e.ubid.toLowerCase().includes(search.toLowerCase()) ||
    e.event_type.toLowerCase().includes(search.toLowerCase()) ||
    e.idempotency_key.toLowerCase().includes(search.toLowerCase()) ||
    e.source_system.toLowerCase().includes(search.toLowerCase()) ||
    e.destination_system.toLowerCase().includes(search.toLowerCase())
  );

  const formatDate = (ts: string) => {
    const dateTs = ts.endsWith('Z') || ts.includes('+') ? ts : `${ts}Z`;
    return new Date(dateTs).toLocaleString('en-IN', {
      day: '2-digit', month: 'short', year: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
    });
  };

  function exportCSV() {
    const rows = [
      ['Timestamp', 'UBID', 'Event Type', 'Source', 'Destination', 'Outcome', 'Payload Hash', 'Idempotency Key'],
      ...filtered.map(e => [
        e.created_at, e.ubid, e.event_type, e.source_system, e.destination_system,
        e.outcome, e.payload_hash, e.idempotency_key,
      ]),
    ];
    const csv = rows.map(r => r.map(v => `"${v}"`).join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `sanchaarsetu-true-audit-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  // Assuming total is unknown since backend GET /audit might just return a list without total.
  // We'll just allow pagination until a page is empty.
  const hasMore = events.length === PER_PAGE;

  return (
    <div className="space-y-6 animate-fade-in">
      {/* Immutability banner */}
      <div className="flex items-start gap-4 card px-6 py-4 border-l-4 border-l-primary bg-primary/5">
        <Lock size={18} className="text-primary flex-shrink-0 mt-0.5" />
        <div>
          <p className="text-[14px] font-bold text-navy">True Append-Only Audit Log · Tamper-Evident</p>
          <p className="text-[12px] text-body mt-1 leading-relaxed">
            This view displays the raw cryptographic <code>audit</code> table. No DELETE or UPDATE permissions exist on this table. Every propagation event is permanently recorded with its payload hash.
          </p>
        </div>
        <button
          onClick={exportCSV}
          className="ml-auto flex-shrink-0 flex items-center gap-1.5 px-4 py-2 bg-white border border-[#e5edf5] hover:bg-gray-50 text-navy text-[13px] rounded-md transition-colors font-semibold shadow-sm"
        >
          <Download size={14} /> Export CSV
        </button>
      </div>

      {/* Controls */}
      <div className="flex items-center gap-4 flex-wrap">
        <div className="relative flex-1 min-w-[280px]">
          <Search size={14} className="absolute left-3.5 top-1/2 -translate-y-1/2 text-muted" />
          <input type="text" placeholder="Search UBID, event, system, idempotency key..."
            value={search} onChange={e => setSearch(e.target.value)}
            className="w-full bg-white border border-[#e5edf5] rounded-md pl-10 pr-4 py-2 text-[13px] text-navy placeholder-muted focus:outline-none focus:ring-1 focus:ring-primary focus:border-primary shadow-sm transition-all" />
        </div>
        <button onClick={load}
          className="flex items-center gap-1.5 px-4 py-2 bg-white hover:bg-gray-50 border border-[#e5edf5] rounded-md text-[13px] text-navy font-medium transition-colors shadow-sm ml-auto">
          <RefreshCw size={14} className={loading ? "animate-spin" : ""} /> Refresh
        </button>
      </div>

      {/* Table */}
      <div className="card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-left">
            <thead>
              <tr className="border-b border-[#e5edf5] bg-gray-50">
                {['Timestamp', 'UBID', 'Event Type', 'Source → Dest', 'Outcome', 'Payload Hash', 'Idempotency Key'].map(h => (
                  <th key={h} className="px-6 py-4 text-[11px] text-body font-bold uppercase tracking-wider whitespace-nowrap">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-[#e5edf5] bg-white">
              {loading && events.length === 0 ? (
                <tr><td colSpan={7} className="py-16 text-center"><RefreshCw size={24} className="animate-spin text-body mx-auto" /></td></tr>
              ) : filtered.length === 0 ? (
                <tr><td colSpan={7} className="py-16 text-center text-body text-[14px]">No records match your filters</td></tr>
              ) : (
                filtered.map(record => {
                  const cfg = OUTCOME_CFG[record.outcome] ?? OUTCOME_CFG.pending;
                  return (
                    <tr key={record.id} className="hover:bg-gray-50/50 transition-colors group">
                      <td className="px-6 py-4 text-[12px] text-muted font-medium whitespace-nowrap">{formatDate(record.created_at)}</td>
                      <td className="px-6 py-4">
                        <code className="text-[13px] text-navy font-semibold font-mono">{record.ubid}</code>
                      </td>
                      <td className="px-6 py-4">
                        <span className="text-[12px] text-navy font-medium capitalize">{record.event_type.replace(/_/g, ' ')}</span>
                      </td>
                      <td className="px-6 py-4">
                        <span className="text-[12px] flex items-center">
                          <span className="text-navy font-medium font-mono bg-gray-100 border border-gray-200 px-2 py-0.5 rounded-md">{record.source_system}</span>
                          <span className="text-muted mx-2">→</span>
                          <span className="text-navy font-medium font-mono bg-gray-100 border border-gray-200 px-2 py-0.5 rounded-md">{record.destination_system}</span>
                        </span>
                      </td>
                      <td className="px-6 py-4">
                        <span className={`text-[11px] px-2.5 py-1 rounded-full border font-semibold ${cfg.bg} ${cfg.border} ${cfg.color}`}>
                          {record.outcome}
                        </span>
                      </td>
                      <td className="px-6 py-4">
                        <code className="text-[11px] text-muted font-mono max-w-[120px] truncate block" title={record.payload_hash}>
                          {record.payload_hash || '—'}
                        </code>
                      </td>
                      <td className="px-6 py-4">
                        <code className="text-[11px] text-muted font-mono max-w-[180px] truncate block" title={record.idempotency_key}>
                          {record.idempotency_key}
                        </code>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>

        <div className="px-6 py-4 border-t border-[#e5edf5] bg-gray-50 flex items-center justify-between text-[12px] text-body font-medium">
          <span>Page {page + 1}</span>
          <div className="flex items-center gap-2">
            <button onClick={() => setPage(p => Math.max(0, p - 1))} disabled={page === 0}
              className="px-3 py-1.5 bg-white border border-[#e5edf5] rounded-md text-navy disabled:opacity-50 hover:bg-gray-50 transition-colors shadow-sm font-semibold">‹ Prev</button>
            <span className="px-4 py-1.5 bg-white border border-[#e5edf5] rounded-md text-primary font-bold shadow-sm">{page + 1}</span>
            <button onClick={() => setPage(p => p + 1)} disabled={!hasMore}
              className="px-3 py-1.5 bg-white border border-[#e5edf5] rounded-md text-navy disabled:opacity-50 hover:bg-gray-50 transition-colors shadow-sm font-semibold">Next ›</button>
          </div>
        </div>
      </div>
    </div>
  );
}

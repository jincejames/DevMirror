import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { listApprovals, approveEdit, rejectEdit } from '../api';
import type { PendingEdit, PendingProvision } from '../types';
import { useUser } from '../UserContext';

export default function AdminApprovals() {
  const { role } = useUser();
  const [pending, setPending] = useState<PendingEdit[]>([]);
  const [provisions, setProvisions] = useState<PendingProvision[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [acting, setActing] = useState<string | null>(null);
  const [rejectFor, setRejectFor] = useState<string | null>(null);
  const [rejectReason, setRejectReason] = useState('');

  function load() {
    setLoading(true);
    listApprovals()
      .then((r) => {
        setPending(r.pending);
        setProvisions(r.pending_provisions ?? []);
      })
      .catch((e) => setError(e instanceof Error ? e.message : 'Failed to load'))
      .finally(() => setLoading(false));
  }

  useEffect(() => {
    load();
  }, []);

  async function handleApprove(id: string) {
    setActing(id);
    try {
      const r = await approveEdit(id);
      alert(r.message);
      load();
    } catch (e) {
      alert(e instanceof Error ? e.message : 'Approve failed');
    } finally {
      setActing(null);
    }
  }

  async function handleReject() {
    if (!rejectFor) return;
    setActing(rejectFor);
    try {
      const r = await rejectEdit(rejectFor, rejectReason);
      alert(r.message);
      setRejectFor(null);
      setRejectReason('');
      load();
    } catch (e) {
      alert(e instanceof Error ? e.message : 'Reject failed');
    } finally {
      setActing(null);
    }
  }

  if (role !== 'admin') {
    return <p className="error-text">Admin access required.</p>;
  }
  if (loading) return <p>Loading...</p>;
  if (error) return <p className="error-text">{error}</p>;

  const totalItems = pending.length + provisions.length;

  return (
    <div>
      <div className="page-header">
        <h1>Approvals</h1>
      </div>

      {totalItems === 0 && (
        <div className="empty-state">
          <p>Nothing waiting on review.</p>
        </div>
      )}

      {/* Section 1: Pending provisions (configs awaiting first-time provision) */}
      {provisions.length > 0 && (
        <div className="detail-section">
          <h2>Pending provisions ({provisions.length})</h2>
          <p style={{ color: '#6b7280', fontSize: '0.85rem', marginTop: 0 }}>
            Configs that have been scanned and are awaiting "Approve &amp; Provision".
          </p>
          <table className="config-table">
            <thead>
              <tr>
                <th>DR</th>
                <th>Description</th>
                <th>Requester</th>
                <th>Scanned</th>
                <th>Objects</th>
                <th>Schemas</th>
                <th>Flags</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {provisions.map((p) => (
                <tr key={p.dr_id}>
                  <td><Link to={`/config/${p.dr_id}/scan`}>{p.dr_id}</Link></td>
                  <td>{p.description || '-'}</td>
                  <td>{p.requested_by}</td>
                  <td>{p.scanned_at ? new Date(p.scanned_at).toLocaleString() : '-'}</td>
                  <td>{p.total_objects}</td>
                  <td>{p.total_schemas}</td>
                  <td style={{ fontSize: '0.8rem' }}>
                    {p.review_required && (
                      <span className="badge badge-expiring_soon">review</span>
                    )}
                    {p.non_prod_additional_objects.length > 0 && (
                      <span
                        className="badge badge-expiring_soon"
                        title={p.non_prod_additional_objects.join('\n')}
                        style={{ marginLeft: 4 }}
                      >
                        non-prod ({p.non_prod_additional_objects.length})
                      </span>
                    )}
                    {!p.review_required && p.non_prod_additional_objects.length === 0 && '-'}
                  </td>
                  <td className="actions">
                    <Link to={`/config/${p.dr_id}/scan`}>
                      <button className="btn-sm">Open Scan Results</button>
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Section 2: Pending edits (sensitive config changes on provisioned DRs) */}
      {pending.length > 0 && (
        <div className="detail-section">
          <h2>Pending edits ({pending.length})</h2>
          <p style={{ color: '#6b7280', fontSize: '0.85rem', marginTop: 0 }}>
            Sensitive config changes (developers, qa_users, additional_objects) on already-provisioned DRs.
          </p>
          <table className="config-table">
            <thead>
              <tr>
                <th>DR</th>
                <th>Requester</th>
                <th>Submitted</th>
                <th>Changes</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {pending.map((p) => (
                <tr key={p.pending_edit_id}>
                  <td>
                    <Link to={`/config/${p.dr_id}`}>{p.dr_id}</Link>
                  </td>
                  <td>{p.requested_by}</td>
                  <td>{p.requested_at ? new Date(p.requested_at).toLocaleString() : '-'}</td>
                  <td>
                    <ul style={{ margin: 0, paddingLeft: '1rem' }}>
                      {p.changes.map((c, i) => (
                        <li key={i} style={{ fontSize: '0.85rem' }}>
                          <code>{c.field}</code>: {fmt(c.before)} -&gt; {fmt(c.after)}
                        </li>
                      ))}
                    </ul>
                  </td>
                  <td className="actions">
                    <button
                      className="btn-sm"
                      disabled={acting === p.pending_edit_id}
                      onClick={() => handleApprove(p.pending_edit_id)}
                    >
                      Approve
                    </button>
                    <button
                      className="btn-sm btn-danger"
                      disabled={acting === p.pending_edit_id}
                      onClick={() => setRejectFor(p.pending_edit_id)}
                    >
                      Reject
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {rejectFor && (
        <div className="dialog-overlay" onClick={() => setRejectFor(null)}>
          <div className="dialog-box" onClick={(e) => e.stopPropagation()}>
            <h3>Reject Edit</h3>
            <p>Optional reason (will be recorded in the audit log):</p>
            <textarea
              value={rejectReason}
              onChange={(e) => setRejectReason(e.target.value)}
              rows={3}
            />
            <div className="dialog-actions">
              <button className="btn-secondary" onClick={() => setRejectFor(null)}>
                Cancel
              </button>
              <button className="btn-danger" onClick={handleReject}>
                Reject
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function fmt(v: unknown): string {
  if (v === null || v === undefined) return '(none)';
  if (Array.isArray(v)) return `[${v.join(', ')}]`;
  // Stringify objects via JSON so a nested change diff doesn't render
  // as the opaque "[object Object]".
  if (typeof v === 'object') {
    try {
      return JSON.stringify(v);
    } catch {
      return '(unserializable)';
    }
  }
  return String(v);
}

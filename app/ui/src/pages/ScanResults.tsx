import { useEffect, useState } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import { getManifest, getConfig, updateManifest, startProvision, rejectConfig } from '../api';
import { useIsAdmin, useUser } from '../UserContext';
import { RejectionBanner } from '../components/RejectionBanner';
import type { ConfigOut, ManifestData, ManifestObject } from '../types';

export default function ScanResults() {
  const { drId } = useParams<{ drId: string }>();
  const navigate = useNavigate();
  const { email } = useUser();
  const isAdmin = useIsAdmin();

  const [manifest, setManifest] = useState<ManifestData | null>(null);
  const [objects, setObjects] = useState<ManifestObject[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [saving, setSaving] = useState(false);
  const [dirty, setDirty] = useState(false);
  const [showConfirm, setShowConfirm] = useState(false);
  const [createdBy, setCreatedBy] = useState('');
  // Rejection state -- only meaningful for admin reviewing the request.
  const [config, setConfig] = useState<ConfigOut | null>(null);
  const [showReject, setShowReject] = useState(false);
  const [rejectComment, setRejectComment] = useState('');
  const [rejecting, setRejecting] = useState(false);

  useEffect(() => {
    if (!drId) return;
    setLoading(true);
    Promise.all([
      getManifest(drId),
      getConfig(drId),
    ])
      .then(([manifestResp, configResp]) => {
        setManifest(manifestResp.manifest);
        setObjects(manifestResp.manifest.objects ?? []);
        setCreatedBy(configResp.created_by ?? '');
        setConfig(configResp);
      })
      .catch((err) => setError(err instanceof Error ? err.message : 'Failed to load manifest'))
      .finally(() => setLoading(false));
  }, [drId]);

  async function handleReject() {
    if (!drId) return;
    const comment = rejectComment.trim();
    if (!comment) {
      alert('Rejection comment cannot be empty.');
      return;
    }
    setRejecting(true);
    try {
      const resp = await rejectConfig(drId, { comment });
      // Reflect the new rejected state in-place so the banner shows up
      // and the action buttons hide; staying on the page lets the admin
      // see the audit-trail message immediately.
      setConfig((prev) =>
        prev
          ? { ...prev,
              status: resp.status,
              rejection_comment: resp.rejection_comment,
              rejected_by: resp.rejected_by,
              rejected_at: resp.rejected_at }
          : prev,
      );
      setShowReject(false);
      setRejectComment('');
    } catch (err) {
      alert(err instanceof Error ? err.message : 'Reject failed');
    } finally {
      setRejecting(false);
    }
  }

  function handleRemove(fqn: string) {
    setObjects((prev) => prev.filter((o) => o.fqn !== fqn));
    setDirty(true);
  }

  async function handleSave() {
    if (!drId || !manifest) return;
    setSaving(true);
    try {
      const updated = { ...manifest, objects };
      const resp = await updateManifest(drId, updated);
      setManifest(resp.manifest);
      setObjects(resp.manifest.objects ?? []);
      setDirty(false);
    } catch (err) {
      alert(err instanceof Error ? err.message : 'Save failed');
    } finally {
      setSaving(false);
    }
  }

  async function handleProvision() {
    if (!drId) return;
    if (dirty) {
      alert('Please save your changes before provisioning.');
      return;
    }
    setShowConfirm(false);
    setSaving(true);
    try {
      const resp = await startProvision(drId);
      navigate(`/config/${drId}/provision/${resp.task_id}`);
    } catch (err) {
      alert(err instanceof Error ? err.message : 'Provision failed');
    } finally {
      setSaving(false);
    }
  }

  const schemas = manifest ? manifest.total_schemas : 0;

  if (loading) return <p>Loading...</p>;
  if (error) return <p className="error-text">{error}</p>;
  if (!manifest) return <p className="error-text">No manifest data.</p>;

  return (
    <div>
      <div className="page-header">
        <h1>Scan Results - {drId}</h1>
        <Link to={`/config/${drId}`}>Back to Config</Link>
      </div>

      {isAdmin && createdBy && createdBy !== email && (
        <div className="owner-label">Owner: {createdBy}</div>
      )}

      {config?.status === 'rejected' && config.rejection_comment && (
        <RejectionBanner
          rejectionComment={config.rejection_comment}
          rejectedBy={config.rejected_by}
          rejectedAt={config.rejected_at}
          guidance="Open the config from the home page, edit it to address the feedback above, and re-save to re-submit for review."
        />
      )}

      {!isAdmin && config?.status !== 'rejected' && (
        <div className="banner banner-info">Scan results are pending admin review.</div>
      )}

      {manifest.review_required && (
        <div className="banner banner-warning">
          Review required: some objects may need manual verification before provisioning.
        </div>
      )}

      {manifest.lineage_row_limit_hit && (
        <div className="banner banner-warning">
          Lineage row limit was hit. Some dependencies may be missing from the scan.
        </div>
      )}

      {manifest.non_prod_additional_objects && manifest.non_prod_additional_objects.length > 0 && (
        <div className="banner banner-warning">
          <strong>{manifest.non_prod_additional_objects.length}</strong>{' '}
          additional objects come from a non-production catalog. Review before approving.
          <ul style={{ margin: '0.5rem 0 0 1rem' }}>
            {manifest.non_prod_additional_objects.map((fqn) => (
              <li key={fqn}><code>{fqn}</code></li>
            ))}
          </ul>
        </div>
      )}

      <div className="detail-section">
        <h2>Streams Scanned</h2>
        <div className="chips">
          {(manifest.streams_scanned ?? []).map((s) => (
            <span key={s} className="chip">{s}</span>
          ))}
        </div>
      </div>

      <div className="summary-cards">
        <div className="summary-card">
          <div className="label">Objects</div>
          <div className="value">{objects.length}</div>
        </div>
        <div className="summary-card">
          <div className="label">Schemas</div>
          <div className="value">{schemas}</div>
        </div>
      </div>

      <table className="object-table">
        <thead>
          <tr>
            <th>FQN</th>
            <th>Type</th>
            <th>Access Mode</th>
            <th>Est. Size (MB)</th>
            <th>Clone Strategy</th>
            {isAdmin && <th></th>}
          </tr>
        </thead>
        <tbody>
          {objects.map((obj) => (
            <tr key={obj.fqn}>
              <td>{obj.fqn}</td>
              <td>{obj.object_type}</td>
              <td>{obj.access_mode}</td>
              <td>{obj.estimated_size_mb ?? '-'}</td>
              <td>{obj.clone_strategy}</td>
              {isAdmin && (
                <td>
                  <button className="btn-sm btn-danger" onClick={() => handleRemove(obj.fqn)}>
                    Remove
                  </button>
                </td>
              )}
            </tr>
          ))}
          {objects.length === 0 && (
            <tr><td colSpan={isAdmin ? 6 : 5} style={{ textAlign: 'center', color: '#6b7280' }}>No objects found.</td></tr>
          )}
        </tbody>
      </table>

      {/* Action row: admin can Save / Approve / Reject while the config
          is still actionable.  A rejected config hides the buttons --
          there's nothing actionable left. */}
      {isAdmin && config?.status !== 'rejected' && (
        <div className="form-actions">
          <button onClick={handleSave} disabled={saving || !dirty}>
            {saving ? 'Saving...' : 'Save Changes'}
          </button>
          <button onClick={() => setShowConfirm(true)} disabled={saving}>
            Approve &amp; Provision
          </button>
          <button
            className="btn-danger"
            onClick={() => setShowReject(true)}
            disabled={saving || rejecting}
          >
            {rejecting ? 'Rejecting...' : 'Reject'}
          </button>
        </div>
      )}

      {isAdmin && showConfirm && (
        <div className="dialog-overlay" onClick={() => setShowConfirm(false)}>
          <div className="dialog-box" onClick={(e) => e.stopPropagation()}>
            <h3>Confirm Provisioning</h3>
            <p>
              Create {schemas} schema(s) and clone {objects.length} object(s)?
              This action will begin provisioning dev/qa environments.
            </p>
            <div className="dialog-actions">
              <button className="btn-secondary" onClick={() => setShowConfirm(false)}>Cancel</button>
              <button onClick={handleProvision}>Confirm</button>
            </div>
          </div>
        </div>
      )}

      {/* Reject dialog -- admin only; required non-empty comment. */}
      {isAdmin && showReject && (
        <div className="dialog-overlay" onClick={() => setShowReject(false)}>
          <div className="dialog-box" onClick={(e) => e.stopPropagation()}>
            <h3>Reject DR {drId}</h3>
            <p>
              The comment below will be shown to the owner. They can edit
              the request to address the feedback and re-submit it.
            </p>
            <div className="form-field">
              <label htmlFor="reject-comment">Reason (required)</label>
              <textarea
                id="reject-comment"
                rows={4}
                value={rejectComment}
                onChange={(e) => setRejectComment(e.target.value)}
                placeholder="e.g. Source catalog isn't approved for the requesting team."
                style={{ width: '100%', boxSizing: 'border-box' }}
              />
            </div>
            <div className="dialog-actions">
              <button
                className="btn-secondary"
                onClick={() => { setShowReject(false); setRejectComment(''); }}
                disabled={rejecting}
              >
                Cancel
              </button>
              <button
                className="btn-danger"
                onClick={handleReject}
                disabled={rejecting || !rejectComment.trim()}
              >
                {rejecting ? 'Rejecting...' : 'Confirm Reject'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

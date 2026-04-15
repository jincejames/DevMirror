import { useEffect, useState } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import { getDrStatus, cleanupDr, refreshDr, reprovisionDr, modifyDr } from '../api';
import { useUser } from '../UserContext';
import type { DrStatusResponse, CleanupResponse, ModifyDrRequest } from '../types';

function statusBadgeClass(status: string): string {
  const key = status.toLowerCase();
  return `badge badge-${key}`;
}

const CLEANABLE = ['ACTIVE', 'EXPIRING_SOON', 'EXPIRED', 'FAILED'];
const REFRESHABLE = ['ACTIVE', 'EXPIRING_SOON', 'FAILED'];

type RefreshMode = 'incremental' | 'full' | 'selective';

export default function DrStatus() {
  const { drId } = useParams<{ drId: string }>();
  const navigate = useNavigate();
  const { role, email } = useUser();
  const [data, setData] = useState<DrStatusResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [showCleanup, setShowCleanup] = useState(false);
  const [cleaning, setCleaning] = useState(false);
  const [cleanupResult, setCleanupResult] = useState<CleanupResponse | null>(null);

  // Refresh state
  const [showRefresh, setShowRefresh] = useState(false);
  const [refreshMode, setRefreshMode] = useState<RefreshMode>('incremental');
  const [selectedObjects, setSelectedObjects] = useState<Set<string>>(new Set());
  const [refreshing, setRefreshing] = useState(false);

  // Re-provision state
  const [showReprovision, setShowReprovision] = useState(false);
  const [reprovisioning, setReprovisioning] = useState(false);

  // Modify state
  const [showModify, setShowModify] = useState(false);
  const [modExpiration, setModExpiration] = useState('');
  const [modAddDevs, setModAddDevs] = useState('');
  const [modRemoveDevs, setModRemoveDevs] = useState('');
  const [modAddQa, setModAddQa] = useState('');
  const [modRemoveQa, setModRemoveQa] = useState('');
  const [modifying, setModifying] = useState(false);
  const [modifySuccess, setModifySuccess] = useState('');

  function load() {
    if (!drId) return;
    setLoading(true);
    setError('');
    getDrStatus(drId)
      .then(setData)
      .catch((err) => setError(err instanceof Error ? err.message : 'Failed to load DR status'))
      .finally(() => setLoading(false));
  }

  useEffect(() => { load(); }, [drId]);

  async function handleCleanup() {
    if (!drId) return;
    setShowCleanup(false);
    setCleaning(true);
    try {
      const result = await cleanupDr(drId);
      setCleanupResult(result);
      load();
    } catch (err) {
      alert(err instanceof Error ? err.message : 'Cleanup failed');
    } finally {
      setCleaning(false);
    }
  }

  async function handleRefresh() {
    if (!drId) return;
    setShowRefresh(false);
    setRefreshing(true);
    try {
      const body: { mode: string; selected_objects?: string[] } = { mode: refreshMode };
      if (refreshMode === 'selective') {
        body.selected_objects = Array.from(selectedObjects);
      }
      const result = await refreshDr(drId, body);
      navigate(`/config/${drId}/provision/${result.task_id}`);
    } catch (err) {
      alert(err instanceof Error ? err.message : 'Refresh failed');
    } finally {
      setRefreshing(false);
    }
  }

  async function handleReprovision() {
    if (!drId) return;
    setShowReprovision(false);
    setReprovisioning(true);
    try {
      const result = await reprovisionDr(drId);
      navigate(`/config/${drId}/provision/${result.task_id}`);
    } catch (err) {
      alert(err instanceof Error ? err.message : 'Re-provision failed');
    } finally {
      setReprovisioning(false);
    }
  }

  function openModifyDialog() {
    if (data) {
      setModExpiration(data.expiration_date);
    }
    setModAddDevs('');
    setModRemoveDevs('');
    setModAddQa('');
    setModRemoveQa('');
    setModifySuccess('');
    setShowModify(true);
  }

  async function handleModify() {
    if (!drId) return;
    setShowModify(false);
    setModifying(true);
    setModifySuccess('');
    try {
      const body: ModifyDrRequest = {};
      if (modExpiration && modExpiration !== data?.expiration_date) {
        body.new_expiration_date = modExpiration;
      }
      const parseEmails = (s: string) =>
        s.split(',').map((e) => e.trim()).filter(Boolean);
      if (modAddDevs.trim()) body.add_developers = parseEmails(modAddDevs);
      if (modRemoveDevs.trim()) body.remove_developers = parseEmails(modRemoveDevs);
      if (modAddQa.trim()) body.add_qa_users = parseEmails(modAddQa);
      if (modRemoveQa.trim()) body.remove_qa_users = parseEmails(modRemoveQa);

      const result = await modifyDr(drId, body);
      setModifySuccess(result.message);
      load();
    } catch (err) {
      alert(err instanceof Error ? err.message : 'Modification failed');
    } finally {
      setModifying(false);
    }
  }

  function toggleSelectObject(fqn: string) {
    setSelectedObjects((prev) => {
      const next = new Set(prev);
      if (next.has(fqn)) {
        next.delete(fqn);
      } else {
        next.add(fqn);
      }
      return next;
    });
  }

  if (loading) return <p>Loading...</p>;
  if (error) return <p className="error-text">{error}</p>;
  if (!data) return <p className="error-text">No data.</p>;

  const isAdmin = role === 'admin';
  const isOwner = data.created_by === email;
  const canModify =
    (isOwner || isAdmin) &&
    ['ACTIVE', 'EXPIRING_SOON'].includes(data.status);
  const canCleanup = isAdmin && CLEANABLE.includes(data.status);
  const canRefresh = (isOwner || isAdmin) && REFRESHABLE.includes(data.status);
  const canReprovision = isAdmin && REFRESHABLE.includes(data.status);

  return (
    <div>
      <div className="page-header">
        <h1>DR Status - {drId}</h1>
        <Link to="/drs">Back to DRs</Link>
      </div>

      {isAdmin && data.created_by && data.created_by !== email && (
        <div className="owner-label">Owner: {data.created_by}</div>
      )}

      {cleanupResult && (
        <div className="banner banner-success">
          Cleanup complete: {cleanupResult.objects_dropped} objects dropped,{' '}
          {cleanupResult.schemas_dropped} schemas dropped,{' '}
          {cleanupResult.revokes_succeeded} revokes succeeded.
        </div>
      )}

      {modifySuccess && (
        <div className="banner banner-success">
          Modification applied: {modifySuccess}
        </div>
      )}

      <div className="detail-section">
        <h2>Details</h2>
        <dl className="detail-grid">
          <dt>DR ID</dt>
          <dd>{data.dr_id}</dd>
          <dt>Status</dt>
          <dd><span className={statusBadgeClass(data.status)}>{data.status}</span></dd>
          <dt>Description</dt>
          <dd>{data.description || '-'}</dd>
          <dt>Expiration</dt>
          <dd>{data.expiration_date}</dd>
          <dt>Created</dt>
          <dd>{data.created_at ? new Date(data.created_at).toLocaleString() : '-'}</dd>
          <dt>Last Refreshed</dt>
          <dd>{data.last_refreshed_at ? new Date(data.last_refreshed_at).toLocaleString() : '-'}</dd>
        </dl>
      </div>

      <div className="summary-cards">
        <div className="summary-card">
          <div className="label">Total Objects</div>
          <div className="value">{data.total_objects}</div>
        </div>
        {Object.entries(data.object_breakdown).map(([status, count]) => (
          <div className="summary-card" key={status}>
            <div className="label">{status}</div>
            <div className="value">{count}</div>
          </div>
        ))}
      </div>

      <div className="detail-section">
        <h2>Objects</h2>
        <table className="object-table">
          <thead>
            <tr>
              {showRefresh && refreshMode === 'selective' && <th>Select</th>}
              <th>Source FQN</th>
              <th>Target FQN</th>
              <th>Status</th>
              <th>Clone Strategy</th>
            </tr>
          </thead>
          <tbody>
            {data.objects.map((obj, i) => (
              <tr key={i}>
                {showRefresh && refreshMode === 'selective' && (
                  <td>
                    <input
                      type="checkbox"
                      checked={selectedObjects.has(obj.source_fqn)}
                      onChange={() => toggleSelectObject(obj.source_fqn)}
                    />
                  </td>
                )}
                <td>{obj.source_fqn}</td>
                <td>{obj.target_fqn}</td>
                <td><span className={statusBadgeClass(obj.status)}>{obj.status}</span></td>
                <td>{obj.clone_strategy}</td>
              </tr>
            ))}
            {data.objects.length === 0 && (
              <tr>
                <td
                  colSpan={showRefresh && refreshMode === 'selective' ? 5 : 4}
                  style={{ textAlign: 'center', color: '#6b7280' }}
                >
                  No objects.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {data.recent_audit.length > 0 && (
        <div className="detail-section">
          <h2>Recent Audit Log</h2>
          <ul className="audit-log">
            {data.recent_audit.map((entry, i) => (
              <li key={i}>
                <span className="audit-timestamp">
                  {entry.timestamp ? new Date(entry.timestamp).toLocaleString() : '-'}
                </span>
                <span className="audit-action">{entry.action}</span>
                {entry.details && <span>{entry.details}</span>}
              </li>
            ))}
          </ul>
        </div>
      )}

      <div className="form-actions">
        {canModify && (
          <button
            className="btn-secondary"
            onClick={openModifyDialog}
            disabled={modifying}
          >
            {modifying ? 'Modifying...' : 'Modify'}
          </button>
        )}
        {canRefresh && (
          <button
            className="btn-secondary"
            onClick={() => setShowRefresh(true)}
            disabled={refreshing}
          >
            {refreshing ? 'Refreshing...' : 'Refresh'}
          </button>
        )}
        {canReprovision && (
          <button
            className="btn-secondary"
            onClick={() => setShowReprovision(true)}
            disabled={reprovisioning}
          >
            {reprovisioning ? 'Re-provisioning...' : 'Re-provision'}
          </button>
        )}
        {canCleanup && (
          <button className="btn-danger" onClick={() => setShowCleanup(true)} disabled={cleaning}>
            {cleaning ? 'Cleaning up...' : 'Cleanup DR'}
          </button>
        )}
      </div>

      {/* Modify Dialog */}
      {showModify && (
        <div className="dialog-overlay" onClick={() => setShowModify(false)}>
          <div className="dialog-box" onClick={(e) => e.stopPropagation()}>
            <h3>Modify DR</h3>
            <p>Update expiration date or manage user access for {drId}.</p>
            <div className="form-field">
              <label htmlFor="mod-expiration">Expiration Date</label>
              <input
                id="mod-expiration"
                type="date"
                value={modExpiration}
                onChange={(e) => setModExpiration(e.target.value)}
              />
            </div>
            <div className="form-field">
              <label htmlFor="mod-add-devs">Add Developers (comma-separated emails)</label>
              <input
                id="mod-add-devs"
                type="text"
                placeholder="user1@example.com, user2@example.com"
                value={modAddDevs}
                onChange={(e) => setModAddDevs(e.target.value)}
              />
            </div>
            <div className="form-field">
              <label htmlFor="mod-remove-devs">Remove Developers (comma-separated emails)</label>
              <input
                id="mod-remove-devs"
                type="text"
                placeholder="user1@example.com"
                value={modRemoveDevs}
                onChange={(e) => setModRemoveDevs(e.target.value)}
              />
            </div>
            <div className="form-field">
              <label htmlFor="mod-add-qa">Add QA Users (comma-separated emails)</label>
              <input
                id="mod-add-qa"
                type="text"
                placeholder="qa1@example.com"
                value={modAddQa}
                onChange={(e) => setModAddQa(e.target.value)}
              />
            </div>
            <div className="form-field">
              <label htmlFor="mod-remove-qa">Remove QA Users (comma-separated emails)</label>
              <input
                id="mod-remove-qa"
                type="text"
                placeholder="qa1@example.com"
                value={modRemoveQa}
                onChange={(e) => setModRemoveQa(e.target.value)}
              />
            </div>
            <div className="dialog-actions">
              <button className="btn-secondary" onClick={() => setShowModify(false)}>Cancel</button>
              <button onClick={handleModify}>Apply Changes</button>
            </div>
          </div>
        </div>
      )}

      {/* Refresh Dialog */}
      {showRefresh && (
        <div className="dialog-overlay" onClick={() => setShowRefresh(false)}>
          <div className="dialog-box" onClick={(e) => e.stopPropagation()}>
            <h3>Refresh DR Objects</h3>
            <p>Re-sync dev objects from production. Choose a refresh mode:</p>
            <div className="form-field">
              <label htmlFor="refresh-mode">Mode</label>
              <select
                id="refresh-mode"
                value={refreshMode}
                onChange={(e) => {
                  setRefreshMode(e.target.value as RefreshMode);
                  setSelectedObjects(new Set());
                }}
              >
                <option value="incremental">Incremental (cloned tables only)</option>
                <option value="full">Full (all objects, drop and recreate)</option>
                <option value="selective">Selective (choose objects)</option>
              </select>
            </div>
            {refreshMode === 'selective' && (
              <div className="form-field">
                <label>Select objects to refresh:</label>
                <div style={{ maxHeight: 200, overflow: 'auto', border: '1px solid #d1d5db', borderRadius: 4, padding: 8 }}>
                  {data.objects.map((obj, i) => (
                    <label key={i} style={{ display: 'block', marginBottom: 4 }}>
                      <input
                        type="checkbox"
                        checked={selectedObjects.has(obj.source_fqn)}
                        onChange={() => toggleSelectObject(obj.source_fqn)}
                      />{' '}
                      {obj.source_fqn}
                    </label>
                  ))}
                </div>
                <small>{selectedObjects.size} object(s) selected</small>
              </div>
            )}
            <div className="dialog-actions">
              <button className="btn-secondary" onClick={() => setShowRefresh(false)}>Cancel</button>
              <button
                onClick={handleRefresh}
                disabled={refreshMode === 'selective' && selectedObjects.size === 0}
              >
                Start Refresh
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Re-provision Dialog */}
      {showReprovision && (
        <div className="dialog-overlay" onClick={() => setShowReprovision(false)}>
          <div className="dialog-box" onClick={(e) => e.stopPropagation()}>
            <h3>Confirm Re-provision</h3>
            <p>
              This will re-scan and re-clone ALL objects for {drId} using the current config.
              Existing cloned objects will be replaced with fresh data from production.
            </p>
            <div className="dialog-actions">
              <button className="btn-secondary" onClick={() => setShowReprovision(false)}>Cancel</button>
              <button onClick={handleReprovision}>Confirm Re-provision</button>
            </div>
          </div>
        </div>
      )}

      {/* Cleanup Dialog */}
      {showCleanup && (
        <div className="dialog-overlay" onClick={() => setShowCleanup(false)}>
          <div className="dialog-box" onClick={(e) => e.stopPropagation()}>
            <h3>Confirm Cleanup</h3>
            <p>
              This will drop all cloned objects, schemas, and revoke grants for {drId}.
              This action cannot be undone.
            </p>
            <div className="dialog-actions">
              <button className="btn-secondary" onClick={() => setShowCleanup(false)} disabled={cleaning}>Cancel</button>
              <button className="btn-danger" onClick={handleCleanup} disabled={cleaning}>
                {cleaning ? 'Cleaning up...' : 'Confirm Cleanup'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

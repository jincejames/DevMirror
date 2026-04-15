import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { listConfigs, deleteConfig, exportYaml, scanConfig, reprovisionDr } from '../api';
import type { ConfigListItem } from '../types';

export default function ConfigList() {
  const [configs, setConfigs] = useState<ConfigListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const navigate = useNavigate();

  async function load() {
    setLoading(true);
    setError('');
    try {
      const resp = await listConfigs();
      const sorted = resp.configs.sort(
        (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime(),
      );
      setConfigs(sorted);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load configs');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { load(); }, []);

  async function handleDelete(drId: string) {
    if (!window.confirm(`Delete config "${drId}"? This cannot be undone.`)) return;
    try {
      await deleteConfig(drId);
      setConfigs((prev) => prev.filter((c) => c.dr_id !== drId));
    } catch (err) {
      alert(err instanceof Error ? err.message : 'Delete failed');
    }
  }

  async function handleExport(drId: string) {
    try {
      const blob = await exportYaml(drId);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${drId}.yaml`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      alert(err instanceof Error ? err.message : 'Export failed');
    }
  }

  const [scanning, setScanning] = useState<string | null>(null);
  const [reprovisioningId, setReprovisioningId] = useState<string | null>(null);

  async function handleScan(drId: string) {
    setScanning(drId);
    try {
      await scanConfig(drId);
      navigate(`/config/${drId}/scan`);
    } catch (err) {
      alert(err instanceof Error ? err.message : 'Scan failed');
    } finally {
      setScanning(null);
    }
  }

  async function handleReprovision(drId: string) {
    if (!window.confirm(`Re-provision "${drId}"? This will re-scan and re-clone ALL objects.`)) return;
    setReprovisioningId(drId);
    try {
      const result = await reprovisionDr(drId);
      navigate(`/config/${drId}/provision/${result.task_id}`);
    } catch (err) {
      alert(err instanceof Error ? err.message : 'Re-provision failed');
    } finally {
      setReprovisioningId(null);
    }
  }

  function statusBadgeClass(status: string) {
    if (status === 'valid') return 'badge badge-valid';
    if (status === 'invalid') return 'badge badge-invalid';
    if (status === 'provisioned') return 'badge badge-provisioned';
    if (status === 'scanned') return 'badge badge-scanned';
    return 'badge badge-draft';
  }

  function workflowStage(status: string): string {
    if (status === 'invalid' || status === 'draft') return 'Draft';
    if (status === 'valid') return 'Valid';
    if (status === 'scanned') return 'Scanned';
    if (status === 'provisioned') return 'Provisioned';
    return status;
  }

  if (loading) return <p>Loading...</p>;
  if (error) return <p className="error-text">{error}</p>;

  return (
    <div>
      <div className="page-header">
        <h1>Development Requests</h1>
        <button onClick={() => navigate('/config/new')}>Create New</button>
      </div>

      {configs.length === 0 ? (
        <div className="empty-state">
          <p>No development requests yet.</p>
          <button onClick={() => navigate('/config/new')}>Create New</button>
        </div>
      ) : (
        <table className="config-table">
          <thead>
            <tr>
              <th>DR ID</th>
              <th>Description</th>
              <th>Status</th>
              <th>Workflow</th>
              <th>Expiration</th>
              <th>Created By</th>
              <th>Created At</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {configs.map((c) => (
              <tr key={c.dr_id}>
                <td>{c.dr_id}</td>
                <td>{c.description || '-'}</td>
                <td><span className={statusBadgeClass(c.status)}>{c.status}</span></td>
                <td>{workflowStage(c.status)}</td>
                <td>{c.expiration_date}</td>
                <td>{c.created_by}</td>
                <td>{new Date(c.created_at).toLocaleString()}</td>
                <td className="actions">
                  {c.status !== 'provisioned' && (
                    <button className="btn-sm" onClick={() => navigate(`/config/${c.dr_id}`)}>
                      Edit
                    </button>
                  )}
                  {c.status === 'valid' && (
                    scanning === c.dr_id ? (
                      <span className="scanning-indicator">
                        <span className="mini-spinner" /> Scanning...
                      </span>
                    ) : (
                      <button className="btn-sm" onClick={() => handleScan(c.dr_id)}>
                        Scan
                      </button>
                    )
                  )}
                  {c.status === 'scanned' && (
                    <>
                      <button className="btn-sm" onClick={() => navigate(`/config/${c.dr_id}/scan`)}>
                        Review
                      </button>
                      {scanning === c.dr_id ? (
                        <span className="scanning-indicator">
                          <span className="mini-spinner" /> Scanning...
                        </span>
                      ) : (
                        <button className="btn-sm btn-secondary" onClick={() => handleScan(c.dr_id)}>
                          Re-scan
                        </button>
                      )}
                    </>
                  )}
                  {c.status === 'provisioned' && (
                    <>
                      <button className="btn-sm" onClick={() => navigate(`/config/${c.dr_id}`)}>
                        Edit
                      </button>
                      <button className="btn-sm" onClick={() => navigate(`/dr/${c.dr_id}`)}>
                        Status
                      </button>
                      {reprovisioningId === c.dr_id ? (
                        <span className="scanning-indicator">
                          <span className="mini-spinner" /> Re-provisioning...
                        </span>
                      ) : (
                        <button className="btn-sm" onClick={() => handleReprovision(c.dr_id)}>
                          Re-provision
                        </button>
                      )}
                    </>
                  )}
                  {c.status !== 'provisioned' && (
                    <button className="btn-sm btn-danger" onClick={() => handleDelete(c.dr_id)}>
                      Delete
                    </button>
                  )}
                  <button className="btn-sm btn-secondary" onClick={() => handleExport(c.dr_id)}>
                    YAML
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

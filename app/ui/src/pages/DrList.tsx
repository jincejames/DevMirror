import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { listDrs } from '../api';
import type { DrListItem } from '../types';

function statusBadgeClass(status: string): string {
  const key = status.toLowerCase();
  return `badge badge-${key}`;
}

export default function DrList() {
  const [drs, setDrs] = useState<DrListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    setLoading(true);
    listDrs()
      .then((resp) => {
        const sorted = resp.drs.sort(
          (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime(),
        );
        setDrs(sorted);
      })
      .catch((err) => setError(err instanceof Error ? err.message : 'Failed to load DRs'))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p>Loading...</p>;
  if (error) return <p className="error-text">{error}</p>;

  return (
    <div>
      <div className="page-header">
        <h1>Active Development Requests</h1>
      </div>

      {drs.length === 0 ? (
        <div className="empty-state">
          <p>No provisioned development requests yet.</p>
        </div>
      ) : (
        <table className="config-table">
          <thead>
            <tr>
              <th>DR ID</th>
              <th>Status</th>
              <th>Description</th>
              <th>Expiration</th>
              <th>Objects</th>
              <th>Created By</th>
            </tr>
          </thead>
          <tbody>
            {drs.map((dr) => (
              <tr key={dr.dr_id}>
                <td>
                  <Link to={`/dr/${dr.dr_id}`}>{dr.dr_id}</Link>
                </td>
                <td><span className={statusBadgeClass(dr.status)}>{dr.status}</span></td>
                <td>{dr.description || '-'}</td>
                <td>{dr.expiration_date}</td>
                <td>{dr.total_objects}</td>
                <td>{dr.created_by}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

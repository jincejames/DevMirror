import { useEffect, useState, useRef } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import { getTaskStatus, startProvision, reprovisionDr } from '../api';
import type { TaskStatusResponse } from '../types';

export default function ProvisionProgress() {
  const { drId, taskId } = useParams<{ drId: string; taskId: string }>();
  const navigate = useNavigate();
  const [task, setTask] = useState<TaskStatusResponse | null>(null);
  const [error, setError] = useState('');
  const [retrying, setRetrying] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    if (!taskId) return;

    function poll() {
      getTaskStatus(taskId!)
        .then((resp) => {
          setTask(resp);
          if (resp.status === 'completed' || resp.status === 'failed') {
            if (intervalRef.current) {
              clearInterval(intervalRef.current);
              intervalRef.current = null;
            }
          }
        })
        .catch((err) => {
          setError(err instanceof Error ? err.message : 'Failed to fetch status');
          if (intervalRef.current) {
            clearInterval(intervalRef.current);
            intervalRef.current = null;
          }
        });
    }

    poll();
    intervalRef.current = setInterval(poll, 3000);
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [taskId]);

  async function handleRetry() {
    if (!drId) return;
    setRetrying(true);
    try {
      // Try reprovision first (for already-provisioned DRs), fall back to provision
      let resp;
      try {
        resp = await reprovisionDr(drId);
      } catch {
        resp = await startProvision(drId);
      }
      navigate(`/config/${drId}/provision/${resp.task_id}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Retry failed');
    } finally {
      setRetrying(false);
    }
  }

  if (!taskId) {
    return (
      <div>
        <div className="page-header">
          <h1>Provisioning - {drId}</h1>
          <Link to="/">Back to Configs</Link>
        </div>
        <div className="banner banner-warning">
          Task ID not available. Check the <Link to={drId ? `/dr/${drId}` : '/'}>DR status page</Link> for the latest state.
        </div>
      </div>
    );
  }

  if (error) return <p className="error-text">{error}</p>;

  const isRunning = task && task.status !== 'completed' && task.status !== 'failed';
  const isTaskDone = task?.status === 'completed' || task?.status === 'failed';
  const result = task?.result as Record<string, unknown> | null;

  // Determine the real outcome: task may have "completed" but the provision result is FAILED
  const provisionFailed = task?.status === 'failed'
    || (result && String(result.final_status) === 'FAILED')
    || (result && Number(result.objects_succeeded ?? 0) === 0 && Number(result.objects_failed ?? 0) > 0);
  const provisionPartial = !provisionFailed && result && Number(result.objects_failed ?? 0) > 0;
  const provisionSuccess = isTaskDone && !provisionFailed && !provisionPartial;

  return (
    <div>
      <div className="page-header">
        <h1>Provisioning - {drId}</h1>
        <Link to="/">Back to Configs</Link>
      </div>

      {(!task || isRunning) && (
        <div className="progress-section">
          <div className="progress-spinner" />
          <h2>Provisioning...</h2>
          <p className="progress-text">{task?.progress || 'Starting...'}</p>
        </div>
      )}

      {isTaskDone && provisionSuccess && result && (
        <div className="banner banner-success">
          <h3 style={{ margin: '0 0 0.5rem 0' }}>Provisioning Succeeded</h3>
          <p style={{ margin: 0 }}>
            Objects: {String(result.objects_succeeded ?? 0)} |
            Schemas: {String(result.schemas_created ?? 0)} |
            Grants: {String(result.grants_applied ?? 0)}
          </p>
        </div>
      )}

      {isTaskDone && provisionPartial && result && (
        <div className="banner banner-warning">
          <h3 style={{ margin: '0 0 0.5rem 0' }}>Provisioning Partially Succeeded</h3>
          <p style={{ margin: 0 }}>
            Objects succeeded: {String(result.objects_succeeded ?? 0)} |
            Objects failed: {String(result.objects_failed ?? 0)} |
            Schemas: {String(result.schemas_created ?? 0)} |
            Grants: {String(result.grants_applied ?? 0)}
          </p>
        </div>
      )}

      {isTaskDone && provisionFailed && (
        <div className="banner banner-error">
          <h3 style={{ margin: '0 0 0.5rem 0' }}>Provisioning Failed</h3>
          {result ? (
            <p style={{ margin: 0 }}>
              Status: {String(result.final_status ?? 'FAILED')} |
              Objects succeeded: {String(result.objects_succeeded ?? 0)} |
              Objects failed: {String(result.objects_failed ?? 0)}
            </p>
          ) : (
            <p style={{ margin: 0 }}>{task?.error || 'An unknown error occurred.'}</p>
          )}
        </div>
      )}

      {task && (
        <div className="detail-section" style={{ marginTop: '1rem' }}>
          <h2>Task Details</h2>
          <dl className="detail-grid">
            <dt>Task ID</dt><dd>{task.task_id}</dd>
            <dt>Type</dt><dd>{task.task_type}</dd>
            <dt>Status</dt>
            <dd>
              <span className={`badge badge-${provisionFailed ? 'failed' : provisionPartial ? 'expiring_soon' : task.status}`}>
                {provisionFailed ? 'FAILED' : provisionPartial ? 'PARTIAL' : task.status.toUpperCase()}
              </span>
            </dd>
            <dt>Started</dt><dd>{new Date(task.started_at).toLocaleString()}</dd>
            {task.completed_at && (<><dt>Completed</dt><dd>{new Date(task.completed_at).toLocaleString()}</dd></>)}
          </dl>
        </div>
      )}

      <div className="form-actions">
        {isTaskDone && provisionFailed && drId && (
          <button onClick={handleRetry} disabled={retrying}>
            {retrying ? 'Retrying...' : 'Retry Provisioning'}
          </button>
        )}
        {isTaskDone && provisionPartial && drId && (
          <button onClick={handleRetry} disabled={retrying}>
            {retrying ? 'Retrying...' : 'Retry Failed Objects'}
          </button>
        )}
        {isTaskDone && drId && (
          <Link to={`/dr/${drId}`}><button className="btn-secondary">View DR Status</button></Link>
        )}
      </div>
    </div>
  );
}

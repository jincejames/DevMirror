import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  createConfig,
  getConfig,
  updateConfig,
  revalidateConfig,
  exportYaml,
  reprovisionDr,
} from '../api';
import type { ConfigIn, FieldError } from '../types';
import MultiInput from '../components/MultiInput';
import RevisionSelector from '../components/RevisionSelector';
import StreamSearch from '../components/StreamSearch';
import ValidationBanner from '../components/ValidationBanner';
import { useUser } from '../UserContext';

// US-34: dr_id is assigned server-side on create; the field is not on
// the form.  When editing we overwrite EMPTY_FORM with the fetched config
// (which includes the already-assigned dr_id).
const EMPTY_FORM: ConfigIn = {
  description: '',
  streams: [],
  additional_objects: [],
  target_catalog: null,
  qa_enabled: false,
  data_revision_mode: 'latest',
  data_revision_version: null,
  data_revision_timestamp: null,
  developers: [],
  qa_users: [],
  expiration_date: '',
  notification_days_before: 7,
  notification_recipients: [],
};

export default function ConfigForm() {
  const { drId } = useParams<{ drId: string }>();
  const isEdit = Boolean(drId);
  const navigate = useNavigate();
  const { role, email } = useUser();

  const [form, setForm] = useState<ConfigIn>({ ...EMPTY_FORM });
  const [errors, setErrors] = useState<FieldError[]>([]);
  const [showBanner, setShowBanner] = useState(false);
  const [isValid, setIsValid] = useState(false);
  const [status, setStatus] = useState('');
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [loadError, setLoadError] = useState('');
  const [reprovisioning, setReprovisioning] = useState(false);
  const [savedBanner, setSavedBanner] = useState(false);
  const [createdBy, setCreatedBy] = useState('');

  const isProvisioned = status === 'provisioned';

  useEffect(() => {
    if (!isEdit || !drId) return;
    setLoading(true);
    getConfig(drId)
      .then((out) => {
        setForm({
          ...out.config,
          streams: out.config.streams ?? [],
          additional_objects: out.config.additional_objects ?? [],
          developers: out.config.developers ?? [],
          qa_users: out.config.qa_users ?? [],
          notification_recipients: out.config.notification_recipients ?? [],
        });
        setCreatedBy(out.created_by ?? '');
        setErrors(out.validation_errors);
        setStatus(out.status);
        setIsValid(out.status === 'valid' || out.status === 'provisioned');
        if (out.validation_errors.length > 0 || out.status === 'valid' || out.status === 'provisioned') {
          setShowBanner(true);
        }
      })
      .catch((err) => setLoadError(err instanceof Error ? err.message : 'Failed to load'))
      .finally(() => setLoading(false));
  }, [isEdit, drId]);

  function fieldError(field: string): string | undefined {
    const match = errors.find(
      (e) => e.loc.includes(field) || e.loc.join('.').includes(field),
    );
    return match?.msg;
  }

  function fieldClass(field: string): string {
    return fieldError(field) ? 'field-error' : '';
  }

  function set<K extends keyof ConfigIn>(key: K, value: ConfigIn[K]) {
    setForm((prev) => ({ ...prev, [key]: value }));
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setSaving(true);
    setShowBanner(false);
    setSavedBanner(false);
    try {
      const payload: ConfigIn = {
        ...form,
        additional_objects:
          form.additional_objects && form.additional_objects.length > 0
            ? form.additional_objects
            : null,
        qa_users: form.qa_enabled && form.qa_users && form.qa_users.length > 0
          ? form.qa_users
          : null,
        notification_recipients:
          form.notification_recipients && form.notification_recipients.length > 0
            ? form.notification_recipients
            : null,
      };

      // US-34: never send dr_id on create -- the server rejects supplied IDs.
      if (!isEdit) {
        delete payload.dr_id;
      }

      const out = isEdit && drId
        ? await updateConfig(drId, payload)
        : await createConfig(payload);

      // 202 path: sensitive edit staged for admin review.
      if ('pending_edit_id' in out) {
        setStatus('pending_review');
        setSavedBanner(true);
        setShowBanner(true);
        setIsValid(true);
        setErrors([]);
        alert('Submitted for admin review.');
        return;
      }

      if (out.status === 'scanned') {
        // Auto-scan completed -- go to review page
        navigate(`/config/${out.dr_id}/scan`);
      } else if (out.status === 'valid' || out.status === 'provisioned') {
        if (isProvisioned) {
          setStatus(out.status);
          setErrors(out.validation_errors);
          setIsValid(true);
          setShowBanner(true);
        } else if (role === 'user') {
          // Non-admin: show pending review banner instead of navigating away.
          // US-34: on a fresh create, jump to the edit route so the
          // server-assigned dr_id is visible in both URL and page title.
          if (!isEdit && out.dr_id) {
            navigate(`/config/${out.dr_id}`, { replace: true });
            return;
          }
          setStatus(out.status);
          setErrors(out.validation_errors);
          setIsValid(true);
          setShowBanner(true);
          setSavedBanner(true);
        } else {
          // Valid but scan didn't run (scan failure is non-blocking)
          navigate('/');
        }
      } else {
        setErrors(out.validation_errors);
        setIsValid(false);
        setStatus(out.status);
        setShowBanner(true);
      }
    } catch (err) {
      console.error('Form submit error:', err);
      const raw = err instanceof Error ? err.message : String(err);
      const mapped: FieldError[] = [];
      try {
        const parsed = JSON.parse(raw);
        // FastAPI 422 format: { detail: [{ loc: ["body","field"], msg: "..." }, ...] }
        if (Array.isArray(parsed.detail)) {
          for (const d of parsed.detail) {
            const loc = Array.isArray(d.loc)
              ? d.loc.filter((p: string) => p !== 'body').map(String)
              : ['general'];
            mapped.push({ loc, msg: d.msg || String(d) });
          }
        } else if (typeof parsed.detail === 'string') {
          mapped.push({ loc: ['general'], msg: parsed.detail });
        }
      } catch {
        mapped.push({ loc: ['general'], msg: raw });
      }
      setErrors(mapped.length > 0 ? mapped : [{ loc: ['general'], msg: raw }]);
      setIsValid(false);
      setShowBanner(true);
    } finally {
      setSaving(false);
    }
  }

  async function handleRevalidate() {
    if (!drId) return;
    setSaving(true);
    try {
      const result = await revalidateConfig(drId);
      setErrors(result.errors);
      setIsValid(result.status === 'valid');
      setStatus(result.status);
      setShowBanner(true);
    } catch (err) {
      setErrors([{ loc: ['general'], msg: err instanceof Error ? err.message : 'Re-validate failed' }]);
      setIsValid(false);
      setShowBanner(true);
    } finally {
      setSaving(false);
    }
  }

  async function handleExport() {
    if (!drId) return;
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

  async function handleReprovision() {
    if (!drId) return;
    // First save current form changes, then re-provision
    setSaving(true);
    setReprovisioning(true);
    try {
      const payload: ConfigIn = {
        ...form,
        additional_objects:
          form.additional_objects && form.additional_objects.length > 0
            ? form.additional_objects
            : null,
        qa_users: form.qa_enabled && form.qa_users && form.qa_users.length > 0
          ? form.qa_users
          : null,
        notification_recipients:
          form.notification_recipients && form.notification_recipients.length > 0
            ? form.notification_recipients
            : null,
      };

      // Save config changes first
      const out = await updateConfig(drId, payload);
      // If the edit was staged for review, do not trigger re-provision.
      if ('pending_edit_id' in out) {
        setStatus('pending_review');
        setSavedBanner(true);
        setShowBanner(true);
        setIsValid(true);
        setErrors([]);
        alert('Submitted for admin review. Re-provision skipped until approval.');
        return;
      }
      if (out.validation_errors.length > 0 && out.status === 'invalid') {
        setErrors(out.validation_errors);
        setIsValid(false);
        setShowBanner(true);
        return;
      }

      // Then trigger re-provision
      const result = await reprovisionDr(drId);
      navigate(`/config/${drId}/provision/${result.task_id}`);
    } catch (err) {
      const raw = err instanceof Error ? err.message : String(err);
      setErrors([{ loc: ['general'], msg: raw }]);
      setIsValid(false);
      setShowBanner(true);
    } finally {
      setSaving(false);
      setReprovisioning(false);
    }
  }

  function handleAdditionalObjectsChange(value: string) {
    const lines = value.split('\n').filter((l) => l.trim() !== '');
    set('additional_objects', lines.length > 0 ? lines : []);
  }

  if (loading) return <p>Loading...</p>;
  if (loadError) return <p className="error-text">{loadError}</p>;

  return (
    <div className="config-form-page">
      <div className="page-header">
        <h1>{isEdit ? `Edit ${drId}` : 'New Development Request'}</h1>
        <button className="btn-secondary" onClick={() => navigate('/')}>
          Back to List
        </button>
      </div>

      {role === 'admin' && createdBy && createdBy !== email && (
        <div className="owner-label">Owner: {createdBy}</div>
      )}

      {isProvisioned && (
        <div className="banner banner-warning">
          This config is already provisioned. Saving will update the config but not the live objects.
          Click &quot;Re-provision&quot; to apply changes.
        </div>
      )}

      {showBanner && <ValidationBanner errors={errors} isValid={isValid} />}

      {savedBanner && (
        <div className="banner banner-info">
          Config saved. Pending admin review for provisioning.
        </div>
      )}

      <form onSubmit={handleSubmit}>
        {/* Section 1: Basic Info
            US-34: the DR ID field is intentionally absent on create; the
            server assigns one, and the page title shows it when editing. */}
        <fieldset className="form-section">
          <legend>Basic Info</legend>
          {isEdit && drId && (
            <div className="form-field">
              <label>DR ID</label>
              <div className="dr-id-display">{drId}</div>
              <small>Auto-generated at creation time.</small>
            </div>
          )}
          <div className="form-field">
            <label htmlFor="description">Description</label>
            <textarea
              id="description"
              value={form.description ?? ''}
              onChange={(e) => set('description', e.target.value || null)}
              rows={3}
              className={fieldClass('description')}
            />
            {fieldError('description') && <span className="field-error-msg">{fieldError('description')}</span>}
          </div>
        </fieldset>

        {/* Section 2: Streams */}
        <fieldset className="form-section">
          <legend>Streams</legend>
          <div className="form-field">
            <label>Search Streams</label>
            <StreamSearch
              selected={form.streams}
              onChange={(val) => set('streams', val)}
              disabled={false}
            />
            {fieldError('streams') && <span className="field-error-msg">{fieldError('streams')}</span>}
          </div>
          <div className="form-field">
            <label htmlFor="additional_objects">Additional Objects (one FQN per line)</label>
            <textarea
              id="additional_objects"
              value={(form.additional_objects ?? []).join('\n')}
              onChange={(e) => handleAdditionalObjectsChange(e.target.value)}
              rows={3}
              placeholder="catalog.schema.table"
              className={fieldClass('additional_objects')}
            />
            {fieldError('additional_objects') && <span className="field-error-msg">{fieldError('additional_objects')}</span>}
          </div>
          <div className="form-field">
            <label htmlFor="target_catalog">Target Catalog</label>
            <input
              id="target_catalog"
              type="text"
              value={form.target_catalog ?? ''}
              onChange={(e) => set('target_catalog', e.target.value || null)}
              placeholder="e.g. dev_analytics (leave empty for auto-derived)"
              className={fieldClass('target_catalog')}
            />
            {fieldError('target_catalog') && <span className="field-error-msg">{fieldError('target_catalog')}</span>}
            <small>The catalog where cloned objects will be created. If empty, derived from source catalog name.</small>
          </div>
        </fieldset>

        {/* Section 3: Environments */}
        <fieldset className="form-section">
          <legend>Environments</legend>
          <div className="form-field">
            <label>
              <input type="checkbox" checked disabled />
              Dev (always enabled)
            </label>
          </div>
          <div className="form-field">
            <label>
              <input
                type="checkbox"
                checked={form.qa_enabled}
                onChange={(e) => set('qa_enabled', e.target.checked)}
              />
              QA
            </label>
          </div>
        </fieldset>

        {/* Section 4: Data Revision */}
        <fieldset className="form-section">
          <legend>Data Revision</legend>
          <RevisionSelector
            mode={form.data_revision_mode}
            version={form.data_revision_version ?? null}
            timestamp={form.data_revision_timestamp ?? null}
            onChange={({ mode, version, timestamp }) => {
              set('data_revision_mode', mode);
              set('data_revision_version', version);
              set('data_revision_timestamp', timestamp);
            }}
            disabled={false}
          />
          {fieldError('data_revision') && <span className="field-error-msg">{fieldError('data_revision')}</span>}
        </fieldset>

        {/* Section 5: Access */}
        <fieldset className="form-section">
          <legend>Access</legend>
          <div className="form-field">
            <label>Developers (required)</label>
            <MultiInput
              values={form.developers}
              onChange={(val) => set('developers', val)}
              placeholder="user@example.com or group-name"
              required
              disabled={false}
            />
            {fieldError('developers') && <span className="field-error-msg">{fieldError('developers')}</span>}
          </div>
          {form.qa_enabled && (
            <div className="form-field">
              <label>QA Users</label>
              <MultiInput
                values={form.qa_users ?? []}
                onChange={(val) => set('qa_users', val)}
                placeholder="qa-user@example.com or group-name"
                disabled={false}
              />
              {fieldError('qa_users') && <span className="field-error-msg">{fieldError('qa_users')}</span>}
            </div>
          )}
        </fieldset>

        {/* Section 6: Lifecycle */}
        <fieldset className="form-section">
          <legend>Lifecycle</legend>
          <div className="form-field">
            <label htmlFor="expiration_date">Expiration Date</label>
            <input
              id="expiration_date"
              type="date"
              value={form.expiration_date}
              onChange={(e) => set('expiration_date', e.target.value)}
              required
              className={fieldClass('expiration_date')}
            />
            {fieldError('expiration_date') && <span className="field-error-msg">{fieldError('expiration_date')}</span>}
            <small>Max 90 days from today</small>
          </div>
          <div className="form-field">
            <label htmlFor="notification_days">Notification days before expiration</label>
            <input
              id="notification_days"
              type="number"
              min={1}
              value={form.notification_days_before}
              onChange={(e) => set('notification_days_before', Number(e.target.value))}
              className={fieldClass('notification_days_before')}
            />
            {fieldError('notification_days_before') && <span className="field-error-msg">{fieldError('notification_days_before')}</span>}
          </div>
          <div className="form-field">
            <label>Notification Recipients</label>
            <MultiInput
              values={form.notification_recipients ?? []}
              onChange={(val) => set('notification_recipients', val)}
              placeholder="Leave empty to use developers list"
              disabled={false}
            />
            {fieldError('notification_recipients') && <span className="field-error-msg">{fieldError('notification_recipients')}</span>}
          </div>
        </fieldset>

        {/* Policy-level errors */}
        {fieldError('policy') && (
          <div className="banner banner-error">{fieldError('policy')}</div>
        )}
        {fieldError('general') && (
          <div className="banner banner-error">{fieldError('general')}</div>
        )}

        {/* Action buttons */}
        <div className="form-actions">
          <button type="submit" disabled={saving}>
            {saving && !reprovisioning ? 'Saving...' : 'Validate & Save'}
          </button>
          {isEdit && !isProvisioned && (
            <button type="button" className="btn-secondary" onClick={handleRevalidate} disabled={saving}>
              Re-validate
            </button>
          )}
          {isEdit && isProvisioned && role === 'admin' && (
            <button
              type="button"
              onClick={handleReprovision}
              disabled={saving || reprovisioning}
            >
              {reprovisioning ? 'Re-provisioning...' : 'Re-provision'}
            </button>
          )}
          {isEdit && (
            <button type="button" className="btn-secondary" onClick={handleExport}>
              Export YAML
            </button>
          )}
        </div>
      </form>
    </div>
  );
}

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
import { RejectionBanner } from '../components/RejectionBanner';
import RevisionSelector from '../components/RevisionSelector';
import StreamSearch from '../components/StreamSearch';
import ValidationBanner from '../components/ValidationBanner';
import { useIsAdmin, useUser } from '../UserContext';

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
  uat_users: [],
  expiration_date: '',
  notification_days_before: 7,
  notification_recipients: [],
};

export default function ConfigForm() {
  const { drId } = useParams<{ drId: string }>();
  const isEdit = Boolean(drId);
  const navigate = useNavigate();
  const { email } = useUser();
  const isAdmin = useIsAdmin();

  const [form, setForm] = useState<ConfigIn>({ ...EMPTY_FORM });
  // Raw textarea content for additional_objects, kept separate from
  // form.additional_objects so Enter/newline keystrokes are preserved
  // verbatim.  We parse it (split on [,\n], trim, drop blanks) into
  // form.additional_objects on every keystroke so submit/validation sees
  // a clean list while the textarea shows exactly what the user typed.
  const [additionalObjectsText, setAdditionalObjectsText] = useState('');
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
  // Rejection metadata is surfaced when status === 'rejected' (admin
  // turned the request down on the Scan Results page).  The owner sees
  // the rationale here on their config detail view.
  const [rejectionComment, setRejectionComment] = useState<string | null>(null);
  const [rejectedBy, setRejectedBy] = useState<string | null>(null);
  const [rejectedAt, setRejectedAt] = useState<string | null>(null);

  const isProvisioned = status === 'provisioned';
  const isRejected = status === 'rejected';

  useEffect(() => {
    if (!isEdit || !drId) return;
    setLoading(true);
    getConfig(drId)
      .then((out) => {
        const loadedAdditional = out.config.additional_objects ?? [];
        setForm({
          ...out.config,
          streams: out.config.streams ?? [],
          additional_objects: loadedAdditional,
          developers: out.config.developers ?? [],
          uat_users: out.config.uat_users ?? [],
          notification_recipients: out.config.notification_recipients ?? [],
        });
        setAdditionalObjectsText(loadedAdditional.join('\n'));
        setCreatedBy(out.created_by ?? '');
        setRejectionComment(out.rejection_comment ?? null);
        setRejectedBy(out.rejected_by ?? null);
        setRejectedAt(out.rejected_at ?? null);
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

    // Client-side description guard.  The server enforces the same rule
    // (min 5 non-blank chars) but checking here gives instant inline
    // feedback and avoids a round-trip on the obvious case.  Whitespace
    // is stripped before length check.
    const descTrimmed = (form.description ?? '').trim();
    if (descTrimmed.length < 5) {
      setErrors([{
        loc: ['description'],
        msg: descTrimmed.length === 0
          ? 'Description is required.'
          : 'Description must be at least 5 characters.',
      }]);
      setShowBanner(true);
      setSaving(false);
      return;
    }

    try {
      const payload: ConfigIn = {
        ...form,
        description: descTrimmed,
        additional_objects:
          form.additional_objects && form.additional_objects.length > 0
            ? form.additional_objects
            : null,
        // UAT users are independent of the qa_enabled toggle: they get
        // SELECT on whatever copies are provisioned (dev + qa when both,
        // dev only when only dev), so the field is always relevant.
        uat_users: form.uat_users && form.uat_users.length > 0
          ? form.uat_users
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
        } else if (!isAdmin) {
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
        // UAT users are independent of the qa_enabled toggle: they get
        // SELECT on whatever copies are provisioned (dev + qa when both,
        // dev only when only dev), so the field is always relevant.
        uat_users: form.uat_users && form.uat_users.length > 0
          ? form.uat_users
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
    // Preserve the user's exact typed text -- including trailing newlines
    // and intermediate blank lines -- by storing it verbatim in
    // additionalObjectsText.  Separately compute a cleaned list for the
    // form state so the rest of the submit/validation pipeline sees a
    // sanitized array.  Accept BOTH newlines and commas as separators so
    // users can paste either form (or a mix).
    setAdditionalObjectsText(value);
    const tokens = value
      .split(/[,\n]/)
      .map((t) => t.trim())
      .filter((t) => t !== '');
    set('additional_objects', tokens);
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

      {isAdmin && createdBy && createdBy !== email && (
        <div className="owner-label">Owner: {createdBy}</div>
      )}

      {isRejected && rejectionComment && (
        <RejectionBanner
          rejectionComment={rejectionComment}
          rejectedBy={rejectedBy}
          rejectedAt={rejectedAt}
          guidance="Edit the request below to address the feedback and re-save to re-submit for review."
        />
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
            <label htmlFor="description">Description (required)</label>
            <textarea
              id="description"
              value={form.description ?? ''}
              onChange={(e) => set('description', e.target.value)}
              rows={3}
              required
              minLength={5}
              className={fieldClass('description')}
            />
            <small>Minimum 5 characters, non-blank.</small>
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
            <label htmlFor="additional_objects">
              Additional Objects <span style={{ fontWeight: 'normal', opacity: 0.7 }}>(one FQN per line OR comma-separated; both work)</span>
            </label>
            <textarea
              id="additional_objects"
              value={additionalObjectsText}
              onChange={(e) => handleAdditionalObjectsChange(e.target.value)}
              rows={5}
              placeholder={'catalog.schema.table1\ncatalog.schema.table2\n\n-- or --\n\ncatalog.schema.table1, catalog.schema.table2'}
              className={fieldClass('additional_objects')}
            />
            {fieldError('additional_objects') && <span className="field-error-msg">{fieldError('additional_objects')}</span>}
          </div>
          <div className="form-field">
            <label htmlFor="target_catalog">
              Override target catalog <span style={{ fontWeight: 'normal', opacity: 0.7 }}>(advanced — leave empty for default routing)</span>
            </label>
            <input
              id="target_catalog"
              type="text"
              value={form.target_catalog ?? ''}
              onChange={(e) => set('target_catalog', e.target.value || null)}
              placeholder="Leave empty unless you need to force every object into one catalog"
              className={fieldClass('target_catalog')}
            />
            {fieldError('target_catalog') && <span className="field-error-msg">{fieldError('target_catalog')}</span>}
            <small>
              <strong>Default (empty):</strong> each object is cloned into <code>&lt;source_base&gt;_n</code> for dev / <code>&lt;source_base&gt;_i</code> for QA — different sources route to their own base catalogs (e.g. <code>odp_adw_ancillaries_p</code> → <code>odp_adw_ancillaries_n</code>).
              <br />
              <strong>If set:</strong> the per-object routing is bypassed and <em>every</em> object in this DR is cloned into the catalog you enter, regardless of source.  Use only when you have a specific reason (e.g. dropping all clones into a shared sandbox catalog).
            </small>
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
          <div className="form-field">
            <label>UAT Users (optional)</label>
            <MultiInput
              values={form.uat_users ?? []}
              onChange={(val) => set('uat_users', val)}
              placeholder="uat-user@example.com or group-name"
              disabled={false}
            />
            {fieldError('uat_users') && <span className="field-error-msg">{fieldError('uat_users')}</span>}
            <small>
              Optional. UAT users get SELECT (read-only) on whichever
              copies get provisioned: dev only if QA is disabled, both dev
              and QA when QA is enabled.
            </small>
          </div>
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
          {isEdit && isProvisioned && isAdmin && (
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

import type { CleanupResponse, CleanupFailure } from '../types';

/**
 * Renders the success / partial-success banner for a cleanup run.
 * Replaces the inline IIFE in DrStatus so the conditional rendering
 * lives next to the data shape it depends on.
 */
interface Props {
  result: CleanupResponse;
}

function FailureList({ label, items }: { label: string; items: CleanupFailure[] }) {
  if (items.length === 0) return null;
  return (
    <details style={{ marginTop: '0.5em' }}>
      <summary>
        {label}: {items.length} couldn&apos;t be processed
      </summary>
      <ul style={{ margin: '0.25em 0 0 1em', padding: 0 }}>
        {items.map((f, i) => (
          <li key={`${label}-${i}`}>
            <code>{f.fqn}</code>: {f.error}
          </li>
        ))}
      </ul>
    </details>
  );
}

export function CleanupResultBanner({ result }: Props) {
  const objF = result.objects_failed ?? [];
  const schF = result.schemas_failed ?? [];
  const revF = result.revokes_failed ?? [];
  const totalFailed = objF.length + schF.length + revF.length;

  if (totalFailed === 0) {
    return (
      <div className="banner banner-success">
        Cleanup complete: {result.objects_dropped} objects dropped,{' '}
        {result.schemas_dropped} schemas dropped,{' '}
        {result.revokes_succeeded} revokes succeeded.
      </div>
    );
  }

  return (
    <div className="banner banner-warning">
      <strong>Partial cleanup</strong> — {totalFailed} item{totalFailed === 1 ? '' : 's'}{' '}
      could not be processed (likely UC permission). Successful:{' '}
      {result.objects_dropped} objects, {result.schemas_dropped} schemas,{' '}
      {result.revokes_succeeded} revokes.
      <FailureList label="Schemas" items={schF} />
      <FailureList label="Objects" items={objF} />
      <FailureList label="Revokes" items={revF} />
    </div>
  );
}

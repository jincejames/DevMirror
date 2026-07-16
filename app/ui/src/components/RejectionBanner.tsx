/**
 * Red banner shown when a config has been rejected by an admin.
 *
 * Surfaces the admin's comment + identity + timestamp, plus
 * page-specific guidance on how to proceed (re-edit + re-save).
 */
interface Props {
  rejectionComment: string;
  rejectedBy?: string | null;
  rejectedAt?: string | null;
  /** Page-specific instruction shown in <small> at the bottom. */
  guidance: string;
}

export function RejectionBanner({ rejectionComment, rejectedBy, rejectedAt, guidance }: Props) {
  return (
    <div className="banner banner-error">
      <strong>This request was rejected.</strong>{' '}
      {rejectedBy && (
        <>
          {rejectedBy}
          {rejectedAt && ` on ${rejectedAt}`}
          {' '}wrote:
        </>
      )}
      <div style={{ marginTop: '0.5em', fontStyle: 'italic' }}>
        &ldquo;{rejectionComment}&rdquo;
      </div>
      <small>{guidance}</small>
    </div>
  );
}

interface RevisionSelectorProps {
  mode: string;
  version: number | null;
  timestamp: string | null;
  onChange: (update: {
    mode: string;
    version: number | null;
    timestamp: string | null;
  }) => void;
  disabled?: boolean;
}

export default function RevisionSelector({
  mode,
  version,
  timestamp,
  onChange,
  disabled = false,
}: RevisionSelectorProps) {
  return (
    <div className="revision-selector">
      <label className="radio-label">
        <input
          type="radio"
          name="data_revision_mode"
          value="latest"
          checked={mode === 'latest'}
          onChange={() => onChange({ mode: 'latest', version: null, timestamp: null })}
          disabled={disabled}
        />
        Latest
      </label>

      <label className="radio-label">
        <input
          type="radio"
          name="data_revision_mode"
          value="version"
          checked={mode === 'version'}
          onChange={() => onChange({ mode: 'version', version: version ?? 1, timestamp: null })}
          disabled={disabled}
        />
        Specific Version
      </label>
      {mode === 'version' && (
        <input
          type="number"
          min={1}
          value={version ?? ''}
          onChange={(e) =>
            onChange({ mode, version: e.target.value ? Number(e.target.value) : null, timestamp: null })
          }
          placeholder="Version number"
          disabled={disabled}
        />
      )}

      <label className="radio-label">
        <input
          type="radio"
          name="data_revision_mode"
          value="timestamp"
          checked={mode === 'timestamp'}
          onChange={() => onChange({ mode: 'timestamp', version: null, timestamp: timestamp ?? '' })}
          disabled={disabled}
        />
        Specific Timestamp
      </label>
      {mode === 'timestamp' && (
        <input
          type="datetime-local"
          value={timestamp ?? ''}
          onChange={(e) =>
            onChange({ mode, version: null, timestamp: e.target.value || null })
          }
          disabled={disabled}
        />
      )}
    </div>
  );
}

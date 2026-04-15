import { useState, useEffect, useRef } from 'react';
import { searchStreams } from '../api';
import type { StreamSearchResult } from '../types';

interface StreamSearchProps {
  selected: string[];
  onChange: (selected: string[]) => void;
  disabled?: boolean;
}

export default function StreamSearch({
  selected,
  onChange,
  disabled = false,
}: StreamSearchProps) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<StreamSearchResult[]>([]);
  const [showDropdown, setShowDropdown] = useState(false);
  const [loading, setLoading] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (timerRef.current) clearTimeout(timerRef.current);

    if (query.length < 2) {
      setResults([]);
      setShowDropdown(false);
      return;
    }

    timerRef.current = setTimeout(async () => {
      setLoading(true);
      try {
        const resp = await searchStreams(query);
        setResults(resp.results.filter((r) => !selected.includes(r.name)));
        setShowDropdown(true);
      } catch {
        setResults([]);
      } finally {
        setLoading(false);
      }
    }, 300);

    return () => {
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, [query, selected]);

  function addStream(name: string) {
    if (!selected.includes(name)) {
      onChange([...selected, name]);
    }
    setQuery('');
    setShowDropdown(false);
  }

  function removeStream(name: string) {
    onChange(selected.filter((s) => s !== name));
  }

  return (
    <div className="stream-search">
      <div className="stream-search-input-wrap">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onBlur={() => setTimeout(() => setShowDropdown(false), 200)}
          placeholder="Search streams (min 2 chars)..."
          disabled={disabled}
        />
        {loading && <span className="search-loading">Searching...</span>}
      </div>

      {showDropdown && results.length > 0 && (
        <ul className="stream-dropdown">
          {results.map((r) => (
            <li key={r.name} onMouseDown={() => addStream(r.name)}>
              <span>{r.name}</span>
              <span className={`badge badge-${r.type}`}>{r.type}</span>
            </li>
          ))}
        </ul>
      )}

      {showDropdown && results.length === 0 && query.length >= 2 && !loading && (
        <div className="stream-dropdown stream-no-results">No results found</div>
      )}

      {selected.length > 0 && (
        <div className="chips">
          {selected.map((name) => (
            <span key={name} className="chip">
              {name}
              {!disabled && (
                <button
                  type="button"
                  className="chip-remove"
                  onClick={() => removeStream(name)}
                  aria-label={`Remove ${name}`}
                >
                  x
                </button>
              )}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

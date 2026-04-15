import { useState } from 'react';

interface MultiInputProps {
  values: string[];
  onChange: (values: string[]) => void;
  placeholder?: string;
  required?: boolean;
  disabled?: boolean;
}

export default function MultiInput({
  values,
  onChange,
  placeholder = '',
  required = false,
  disabled = false,
}: MultiInputProps) {
  const [input, setInput] = useState('');

  function add() {
    const trimmed = input.trim();
    if (trimmed && !values.includes(trimmed)) {
      onChange([...values, trimmed]);
    }
    setInput('');
  }

  function remove(val: string) {
    onChange(values.filter((v) => v !== val));
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'Enter') {
      e.preventDefault();
      add();
    }
  }

  return (
    <div className="multi-input">
      <div className="multi-input-row">
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          required={required && values.length === 0}
          disabled={disabled}
        />
        <button type="button" onClick={add} disabled={disabled || !input.trim()}>
          Add
        </button>
      </div>
      {values.length > 0 && (
        <div className="chips">
          {values.map((v) => (
            <span key={v} className="chip">
              {v}
              {!disabled && (
                <button
                  type="button"
                  className="chip-remove"
                  onClick={() => remove(v)}
                  aria-label={`Remove ${v}`}
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

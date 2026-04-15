import type { FieldError } from '../types';

interface ValidationBannerProps {
  errors: FieldError[];
  isValid: boolean;
}

export default function ValidationBanner({ errors, isValid }: ValidationBannerProps) {
  if (isValid) {
    return <div className="banner banner-success">Configuration valid</div>;
  }
  return (
    <div className="banner banner-error">
      {errors.length} error{errors.length !== 1 ? 's' : ''} found
    </div>
  );
}

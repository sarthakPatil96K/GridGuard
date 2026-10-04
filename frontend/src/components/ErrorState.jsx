import { RefreshCw } from "lucide-react";

function ErrorState({
  message = "Unable to load data.",
  onRetry,
}) {
  return (
    <div className="error-state">
      <div className="error-icon">!</div>

      <h3>Something went wrong</h3>

      <p>{message}</p>

      {onRetry && (
        <button
          className="retry-button"
          onClick={onRetry}
        >
          <RefreshCw size={16} />
          Retry
        </button>
      )}
    </div>
  );
}

export default ErrorState;
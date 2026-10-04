function LoadingState({ message = "Loading data..." }) {
  return (
    <div className="loading-state">
      <div className="loading-spinner"></div>
      <span>{message}</span>
    </div>
  );
}

export default LoadingState;
import { Database } from "lucide-react";

function EmptyState({
  title = "No data available",
  message = "There is no data to display.",
}) {
  return (
    <div className="empty-state">
      <Database size={38} />

      <h3>{title}</h3>

      <p>{message}</p>
    </div>
  );
}

export default EmptyState;
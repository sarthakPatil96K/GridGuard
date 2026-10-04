import { useEffect, useState } from "react";
import { AlertTriangle, ShieldAlert, Clock, CheckCircle2 } from "lucide-react";
import { getAlerts } from "../services/api";

function Alerts() {
  const [alerts, setAlerts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    async function loadAlerts() {
      try {
        setLoading(true);
        setError("");

        const response = await getAlerts();

        const data = Array.isArray(response?.data)
          ? response.data
          : [];

        setAlerts(data);
      } catch (err) {
        console.error("Alerts loading error:", err);
        setError(
          "Unable to load alerts. Make sure the FastAPI server is running."
        );
      } finally {
        setLoading(false);
      }
    }

    loadAlerts();
  }, []);

  function formatDate(value) {
    if (!value) return "—";

    const date = new Date(value);

    if (Number.isNaN(date.getTime())) {
      return value;
    }

    return date.toLocaleString("en-IN", {
      day: "2-digit",
      month: "short",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  }

  function getStatusClass(status) {
    switch (String(status || "").toLowerCase()) {
      case "pending":
        return "alert-status-pending";

      case "reviewed":
        return "alert-status-reviewed";

      case "resolved":
        return "alert-status-resolved";

      default:
        return "alert-status-default";
    }
  }

  if (loading) {
    return (
      <div className="loading-state">
        <div className="loading-spinner"></div>
        <span>Loading investigation alerts...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="error-state">
        <div className="error-icon">!</div>
        <h3>Alerts unavailable</h3>
        <p>{error}</p>
      </div>
    );
  }

  const pendingCount = alerts.filter(
    (alert) =>
      String(alert.status || "").toLowerCase() === "pending"
  ).length;

  const reviewedCount = alerts.filter(
    (alert) =>
      String(alert.status || "").toLowerCase() === "reviewed"
  ).length;

  const resolvedCount = alerts.filter(
    (alert) =>
      String(alert.status || "").toLowerCase() === "resolved"
  ).length;

  return (
    <div className="dashboard-page">
      <div className="page-heading">
        <div>
          <h2>Investigation Alerts</h2>
          <p>
            High-priority meters requiring further field investigation.
          </p>
        </div>

        <div className="live-indicator">
          <span></span>
          Alert Monitoring Active
        </div>
      </div>

      {/* Summary */}

      <div className="kpi-grid">
        <div className="kpi-card">
          <div className="kpi-icon">
            <ShieldAlert size={22} />
          </div>

          <div>
            <span className="kpi-title">Total Alerts</span>
            <strong className="kpi-value">{alerts.length}</strong>
            <span className="kpi-subtitle">
              Investigation flags
            </span>
          </div>
        </div>

        <div className="kpi-card">
          <div className="kpi-icon">
            <Clock size={22} />
          </div>

          <div>
            <span className="kpi-title">Pending</span>
            <strong className="kpi-value">{pendingCount}</strong>
            <span className="kpi-subtitle">
              Awaiting review
            </span>
          </div>
        </div>

        <div className="kpi-card">
          <div className="kpi-icon">
            <AlertTriangle size={22} />
          </div>

          <div>
            <span className="kpi-title">Reviewed</span>
            <strong className="kpi-value">{reviewedCount}</strong>
            <span className="kpi-subtitle">
              Reviewed by investigator
            </span>
          </div>
        </div>

        <div className="kpi-card">
          <div className="kpi-icon">
            <CheckCircle2 size={22} />
          </div>

          <div>
            <span className="kpi-title">Resolved</span>
            <strong className="kpi-value">{resolvedCount}</strong>
            <span className="kpi-subtitle">
              Investigation completed
            </span>
          </div>
        </div>
      </div>

      {/* Alerts table */}

      <div className="panel alert-panel">
        <div className="panel-header">
          <div>
            <h3>Investigation Queue</h3>
            <p>
              Alerts generated from high investigation-priority
              meters.
            </p>
          </div>

          <ShieldAlert size={20} />
        </div>

        {alerts.length === 0 ? (
          <div className="empty-priority-state">
            <CheckCircle2 size={32} />

            <strong>No active investigation alerts</strong>

            <span>
              GridGuard currently has no high-priority alerts.
            </span>
          </div>
        ) : (
          <div className="priority-table-wrapper">
            <table className="priority-table alert-table">
              <thead>
                <tr>
                  <th>Alert</th>
                  <th>Meter</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Created</th>
                  <th>Action</th>
                </tr>
              </thead>

              <tbody>
                {alerts.map((alert, index) => (
                  <tr key={alert.id ?? `${alert.meter_id}-${index}`}>
                    <td>
                      <div className="alert-title-cell">
                        <AlertTriangle size={17} />
                        <span>
                          Investigation Required
                        </span>
                      </div>
                    </td>

                    <td className="meter-id-cell">
                      {alert.meter_id}
                    </td>

                    <td>
                      {String(
                        alert.alert_type || "INVESTIGATION"
                      )
                        .replaceAll("_", " ")
                        .toUpperCase()}
                    </td>

                    <td>
                      <span
                        className={`alert-status ${getStatusClass(
                          alert.status
                        )}`}
                      >
                        {String(
                          alert.status || "pending"
                        ).toUpperCase()}
                      </span>
                    </td>

                    <td>{formatDate(alert.created_at)}</td>

                    <td>
                      <button
                        className="alert-action-button"
                        onClick={() =>
                          window.location.href = `/meters/${alert.meter_id}`
                        }
                      >
                        View Meter
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        <div className="priority-disclaimer">
          <ShieldAlert size={18} />

          <span>
            Alerts indicate meters whose consumption behavior has
            generated strong abnormality evidence. They are
            investigation recommendations and do not confirm
            electricity theft.
          </span>
        </div>
      </div>
    </div>
  );
}

export default Alerts;
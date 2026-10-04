import axios from "axios";

const api = axios.create({
  baseURL: "http://127.0.0.1:8000/api",
  headers: {
    "Content-Type": "application/json",
  },
});

// =========================================================
// DASHBOARD
// =========================================================

export const getDashboardSummary = () =>
  api.get("/dashboard/summary");

export const getTrends = () =>
  api.get("/analytics/trends");

export const getRiskDistribution = () =>
  api.get("/analytics/anomaly-types");


// =========================================================
// METERS
// =========================================================

export const getMeters = () =>
  api.get("/meters/");

export const getMeter = (meterId) =>
  api.get(`/meters/${meterId}`);

export const getMeterConsumption = (meterId) =>
  api.get(`/meters/${meterId}/consumption`);


// =========================================================
// ANOMALIES
// =========================================================

export const getAnomalies = () =>
  api.get("/anomalies/");

export const getHighRiskAnomalies = () =>
  api.get("/anomalies/high-risk");

export const getAnomalyTypes = () =>
  api.get("/analytics/anomaly-types");


// =========================================================
// ALERTS
// =========================================================

export const getAlerts = () =>
  api.get("/alerts/");


// =========================================================
// INVESTIGATION PRIORITIES
// =========================================================

export const getPriorities = () =>
  api.get("/priorities/");
export const getMeterPriority = (meterId) =>
  api.get(`/priorities/${meterId}`);

export default api;
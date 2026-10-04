import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";

import {
  ArrowLeft,
  Gauge,
  AlertTriangle,
  Activity,
  Moon,
  TrendingUp,
  ShieldAlert,
  Clock3,
  BarChart3,
  Target,
  Layers3,
  CalendarDays,
} from "lucide-react";

import {
  getMeter,
  getMeterConsumption,
  getMeterPriority,
} from "../services/api";

import RiskBadge from "../components/RiskBadge";

import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  AreaChart,
  Area,
  Legend,
} from "recharts";

/* =========================================================
   NORMALIZE API RESPONSE
========================================================= */

function extractConsumptionRows(response) {
  if (Array.isArray(response)) {
    return response;
  }

  if (Array.isArray(response?.data)) {
    return response.data;
  }

  if (Array.isArray(response?.results)) {
    return response.results;
  }

  if (Array.isArray(response?.consumption)) {
    return response.consumption;
  }

  if (Array.isArray(response?.history)) {
    return response.history;
  }

  return [];
}

/* =========================================================
   BUILD 7-DAY HISTORICAL BASELINE
========================================================= */

function addBaseline(rows) {
  return rows.map((item, index) => {
    /*
     * Use previous 7 observations.
     *
     * We intentionally exclude the current
     * day's consumption from the baseline.
     */

    const previousRows = rows.slice(
      Math.max(0, index - 7),
      index
    );

    const previousValues = previousRows
      .map((row) => Number(row.consumption))
      .filter((value) => Number.isFinite(value));

    let baseline = null;

    if (previousValues.length > 0) {
      baseline =
        previousValues.reduce(
          (sum, value) => sum + value,
          0
        ) / previousValues.length;
    }

    return {
      ...item,
      baseline:
        baseline !== null
          ? Number(baseline.toFixed(3))
          : null,
    };
  });
}

/* =========================================================
   NORMALIZE CONSUMPTION DATA
========================================================= */

function normalizeConsumptionData(response) {
  const rows = extractConsumptionRows(response);

  console.log(
    "Raw meter consumption response:",
    response
  );

  console.log(
    "Raw consumption rows:",
    rows
  );

  const normalized = rows
    .map((item) => {
      /* -------------------------------
         DATE
      -------------------------------- */

      const date =
        item.date ??
        item.analysis_date ??
        item.Date ??
        item.datetime ??
        item.DateTime ??
        item.timestamp;

      /* -------------------------------
         CONSUMPTION
      -------------------------------- */

      const rawConsumption =
        item.daily_consumption ??
        item.consumption ??
        item.avg_consumption ??
        item.average_consumption ??
        item.total_consumption ??
        item.daily_kwh ??
        item.kwh ??
        item.value;

      const consumption = Number(rawConsumption);

      return {
        date,
        consumption,
      };
    })
    .filter(
      (item) =>
        item.date &&
        Number.isFinite(item.consumption)
    )
    .sort(
      (a, b) =>
        new Date(a.date) -
        new Date(b.date)
    );

  const result = addBaseline(normalized);

  console.log(
    "Normalized meter consumption:",
    result
  );

  return result;
}

/* =========================================================
   NORMALIZE PRIORITY DATA
========================================================= */

function normalizePriorityData(response) {
  if (!response) {
    return null;
  }

  const data = response?.data ?? response;

  /*
   * The priority endpoint returns one object for:
   * GET /priorities/{meter_id}
   */

  if (
    data &&
    typeof data === "object" &&
    !Array.isArray(data)
  ) {
    return {
      meter_id:
        data.meter_id ??
        data.meter ??
        null,

      investigation_priority_score: Number(
        data.investigation_priority_score ??
          data.priority_score ??
          0
      ),

      priority_level: String(
        data.priority_level ??
          data.level ??
          "LOW"
      ).toUpperCase(),

      analyzed_days: Number(
        data.analyzed_days ?? 0
      ),

      anomaly_days: Number(
        data.anomaly_days ?? 0
      ),

      high_risk_days: Number(
        data.high_risk_days ?? 0
      ),

      avg_event_risk_score: Number(
        data.avg_event_risk_score ?? 0
      ),

      max_event_risk_score: Number(
        data.max_event_risk_score ?? 0
      ),

      anomaly_rate: Number(
        data.anomaly_rate ?? 0
      ),

      persistence_score: Number(
        data.persistence_score ?? 0
      ),

      frequency_score: Number(
        data.frequency_score ?? 0
      ),

      model_agreement_days: Number(
        data.model_agreement_days ?? 0
      ),

      agreement_rate: Number(
        data.agreement_rate ??
          data.agreement_score ??
          0
      ),

      agreement_score: Number(
        data.agreement_score ??
          data.agreement_rate ??
          0
      ),

      recent_anomaly_days: Number(
        data.recent_anomaly_days ?? 0
      ),

      recent_activity_score: Number(
        data.recent_activity_score ?? 0
      ),

      severity_score: Number(
        data.severity_score ?? 0
      ),
    };
  }

  return null;
}

/* =========================================================
   DATE FORMATTERS
========================================================= */

function formatAxisDate(value) {
  if (!value) {
    return "";
  }

  const date = new Date(value);

  if (
    Number.isNaN(
      date.getTime()
    )
  ) {
    return value;
  }

  return date.toLocaleDateString(
    "en-IN",
    {
      month: "short",
      year: "numeric",
    }
  );
}

function formatTooltipDate(value) {
  if (!value) {
    return "";
  }

  const date = new Date(value);

  if (
    Number.isNaN(
      date.getTime()
    )
  ) {
    return value;
  }

  return date.toLocaleDateString(
    "en-IN",
    {
      day: "2-digit",
      month: "short",
      year: "numeric",
    }
  );
}

/* =========================================================
   PRIORITY HELPERS
========================================================= */

function getPriorityClass(level) {
  switch (
    String(level || "")
      .toUpperCase()
      .trim()
  ) {
    case "VERY HIGH":
      return "priority-very-high";

    case "HIGH":
      return "priority-high";

    case "MEDIUM":
      return "priority-medium";

    case "LOW":
      return "priority-low";

    default:
      return "priority-normal";
  }
}

/* =========================================================
   METER DETAILS
========================================================= */

function MeterDetails() {
  const { meterId } = useParams();

  const [meter, setMeter] =
    useState(null);

  const [consumption, setConsumption] =
    useState([]);

  const [priority, setPriority] =
    useState(null);

  const [priorityLoading, setPriorityLoading] =
    useState(true);

  const [priorityError, setPriorityError] =
    useState("");

  const [loading, setLoading] =
    useState(true);

  const [error, setError] =
    useState("");

  /* =======================================================
     LOAD DATA
  ======================================================= */

  useEffect(() => {
    async function loadMeter() {
      try {
        setLoading(true);
        setError("");
        setPriorityLoading(true);
        setPriorityError("");

        /*
         * Load all three datasets together.
         *
         * Priority is treated separately so that a
         * priority API problem does not destroy the
         * existing Meter Details page.
         */

        const [
          meterResult,
          consumptionResult,
          priorityResult,
        ] = await Promise.allSettled([
          getMeter(meterId),
          getMeterConsumption(meterId),
          getMeterPriority(meterId),
        ]);

        /* -------------------------------
           Meter information
        -------------------------------- */

        if (
          meterResult.status === "fulfilled"
        ) {
          setMeter(
            meterResult.value?.data ??
              meterResult.value
          );
        } else {
          throw meterResult.reason;
        }

        /* -------------------------------
           Consumption history
        -------------------------------- */

        if (
          consumptionResult.status ===
          "fulfilled"
        ) {
          const normalizedConsumption =
            normalizeConsumptionData(
              consumptionResult.value?.data ??
                consumptionResult.value
            );

          setConsumption(
            normalizedConsumption
          );
        } else {
          console.warn(
            "Consumption history could not be loaded:",
            consumptionResult.reason
          );

          setConsumption([]);
        }

        /* -------------------------------
           Investigation priority
        -------------------------------- */

        if (
          priorityResult.status ===
          "fulfilled"
        ) {
          const normalizedPriority =
            normalizePriorityData(
              priorityResult.value
            );

          setPriority(
            normalizedPriority
          );
        } else {
          console.warn(
            "Investigation priority could not be loaded:",
            priorityResult.reason
          );

          setPriority(null);

          setPriorityError(
            "Investigation-priority evidence is currently unavailable."
          );
        }
      } catch (err) {
        console.error(
          "Meter details error:",
          err
        );

        setError(
          "Unable to load meter details."
        );
      } finally {
        setPriorityLoading(false);
        setLoading(false);
      }
    }

    if (meterId) {
      loadMeter();
    }
  }, [meterId]);

  /* =======================================================
     LOADING
  ======================================================= */

  if (loading) {
    return (
      <div className="loading-state">
        <div className="loading-spinner"></div>

        <span>
          Loading meter details...
        </span>
      </div>
    );
  }

  /* =======================================================
     ERROR
  ======================================================= */

  if (error || !meter) {
    return (
      <div className="error-state">
        <div className="error-icon">
          !
        </div>

        <h3>
          Meter unavailable
        </h3>

        <p>
          {error ||
            "Meter not found."}
        </p>

        <Link
          to="/meters"
          className="back-link"
        >
          <ArrowLeft size={17} />
          Back to Meters
        </Link>
      </div>
    );
  }

  /* =======================================================
     RISK SCORE
  ======================================================= */

  const riskScore =
    Number(
      meter.risk_score || 0
    );

  /* =======================================================
     PRIORITY VALUES
  ======================================================= */

  const priorityScore =
    Number(
      priority?.investigation_priority_score ??
        0
    );

  const priorityLevel =
    priority?.priority_level ??
    "LOW";

  /* =======================================================
     RENDER
  ======================================================= */

  return (
    <div className="meter-details-page">

      {/* ===================================================
          HEADER
      =================================================== */}

      <div className="details-header">

        <div>

          <Link
            to="/meters"
            className="back-link"
          >
            <ArrowLeft
              size={17}
            />

            Back to Meters
          </Link>

          <div className="meter-title">

            <div className="meter-title-icon">
              <Gauge
                size={25}
              />
            </div>

            <div>

              <h2>
                {meterId}
              </h2>

              <p>
                Smart meter consumption
                profile
              </p>

            </div>

          </div>

        </div>

        <RiskBadge
          level={
            meter.risk_level ||
            "LOW"
          }
        />

      </div>


      {/* ===================================================
          RISK OVERVIEW
      =================================================== */}

      <div className="details-grid">

        <div className="risk-overview-card">

          <div className="risk-overview-header">

            <div>

              <span>
                Current Risk Score
              </span>

              <h1>
                {riskScore.toFixed(1)}
              </h1>

            </div>

            <ShieldAlert
              size={28}
            />

          </div>

          <div className="risk-progress">

            <div
              style={{
                width: `${Math.min(
                  riskScore,
                  100
                )}%`,
              }}
            />

          </div>

          <p>
            Project-defined analytical
            score from 0–100. This score
            identifies unusual consumption
            behavior and does not establish
            electricity theft.
          </p>

        </div>


        {/* METER INFORMATION */}

        <div className="detail-info-card">

          <h3>
            Meter Information
          </h3>

          <div className="info-row">

            <span>
              Meter ID
            </span>

            <strong>
              {meterId}
            </strong>

          </div>

          <div className="info-row">

            <span>
              Zone
            </span>

            <strong>
              {meter.zone ||
                "—"}
            </strong>

          </div>

          <div className="info-row">

            <span>
              Consumer Type
            </span>

            <strong>
              {meter.consumer_type ||
                "Residential"}
            </strong>

          </div>

          <div className="info-row">

            <span>
              Status
            </span>

            <span className="status-active">
              Active
            </span>

          </div>

        </div>

      </div>


      {/* ===================================================
          METRICS
      =================================================== */}

      <div className="detail-metrics">

        <div className="detail-metric">

          <Activity
            size={21}
          />

          <div>

            <span>
              Average Consumption
            </span>

            <strong>

              {meter.avg_consumption != null
                ? Number(
                    meter.avg_consumption
                  ).toFixed(3)
                : "—"}

              {meter.avg_consumption !=
                null &&
                " kWh"}

            </strong>

          </div>

        </div>


        <div className="detail-metric">

          <TrendingUp
            size={21}
          />

          <div>

            <span>
              Historical Deviation
            </span>

            <strong>

              {meter.historical_deviation !=
              null
                ? Number(
                    meter.historical_deviation
                  ).toFixed(2)
                : "—"}

            </strong>

          </div>

        </div>


        <div className="detail-metric">

          <Moon
            size={21}
          />

          <div>

            <span>
              Night Consumption Ratio
            </span>

            <strong>

              {meter.night_consumption_ratio !=
              null
                ? `${(
                    Number(
                      meter.night_consumption_ratio
                    ) * 100
                  ).toFixed(1)}%`
                : "—"}

            </strong>

          </div>

        </div>


        <div className="detail-metric">

          <AlertTriangle
            size={21}
          />

          <div>

            <span>
              Anomaly Count
            </span>

            <strong>
              {meter.anomaly_count ??
                "—"}
            </strong>

          </div>

        </div>

      </div>


      {/* ===================================================
          INVESTIGATION EVIDENCE
      =================================================== */}

      <div className="panel investigation-evidence-panel">

        <div className="panel-header">

          <div>

            <h3>
              Investigation Evidence
            </h3>

            <p>
              Analytical evidence supporting
              the investigation priority assigned
              to this meter.
            </p>

          </div>

          <ShieldAlert
            size={20}
          />

        </div>


        {priorityLoading ? (

          <div className="priority-loading-state">

            <div className="loading-spinner"></div>

            <span>
              Loading investigation evidence...
            </span>

          </div>

        ) : priority ? (

          <>

            {/* SCORE HEADER */}

            <div className="investigation-score-section">

              <div>

                <span className="evidence-label">
                  Investigation Priority Score
                </span>

                <div className="investigation-score">

                  {priorityScore.toFixed(2)}

                  <span>
                    /100
                  </span>

                </div>

              </div>

              <span
                className={`priority-badge ${getPriorityClass(
                  priorityLevel
                )}`}
              >
                {priorityLevel}
              </span>

            </div>


            {/* EVIDENCE METRICS */}

            <div className="evidence-grid">

              <div className="evidence-card">

                <div className="evidence-card-icon">
                  <AlertTriangle
                    size={18}
                  />
                </div>

                <span>
                  Anomaly Days
                </span>

                <strong>
                  {priority.anomaly_days}
                </strong>

                <small>
                  Days with abnormal
                  consumption signals
                </small>

              </div>


              <div className="evidence-card">

                <div className="evidence-card-icon">
                  <ShieldAlert
                    size={18}
                  />
                </div>

                <span>
                  High-Risk Days
                </span>

                <strong>
                  {priority.high_risk_days}
                </strong>

                <small>
                  Days with elevated
                  anomaly risk
                </small>

              </div>


              <div className="evidence-card">

                <div className="evidence-card-icon">
                  <Target
                    size={18}
                  />
                </div>

                <span>
                  Model Agreement
                </span>

                <strong>
                  {priority.agreement_rate.toFixed(
                    1
                  )}
                  %
                </strong>

                <small>
                  Agreement across
                  detection signals
                </small>

              </div>


              <div className="evidence-card">

                <div className="evidence-card-icon">
                  <CalendarDays
                    size={18}
                  />
                </div>

                <span>
                  Analyzed Days
                </span>

                <strong>
                  {priority.analyzed_days}
                </strong>

                <small>
                  Historical days analyzed
                </small>

              </div>


              <div className="evidence-card">

                <div className="evidence-card-icon">
                  <Clock3
                    size={18}
                  />
                </div>

                <span>
                  Persistence
                </span>

                <strong>
                  {priority.persistence_score.toFixed(
                    1
                  )}
                </strong>

                <small>
                  Consistency of abnormal
                  behavior
                </small>

              </div>


              <div className="evidence-card">

                <div className="evidence-card-icon">
                  <BarChart3
                    size={18}
                  />
                </div>

                <span>
                  Frequency
                </span>

                <strong>
                  {priority.frequency_score.toFixed(
                    1
                  )}
                </strong>

                <small>
                  Frequency of abnormal
                  events
                </small>

              </div>


              <div className="evidence-card">

                <div className="evidence-card-icon">
                  <Layers3
                    size={18}
                  />
                </div>

                <span>
                  Severity
                </span>

                <strong>
                  {priority.severity_score.toFixed(
                    1
                  )}
                </strong>

                <small>
                  Strength of abnormal
                  deviations
                </small>

              </div>


              <div className="evidence-card">

                <div className="evidence-card-icon">
                  <Activity
                    size={18}
                  />
                </div>

                <span>
                  Recent Activity
                </span>

                <strong>
                  {priority.recent_activity_score.toFixed(
                    1
                  )}
                </strong>

                <small>
                  Recent abnormal activity
                  indicator
                </small>

              </div>

            </div>


            {/* ADDITIONAL EVIDENCE */}

            <div className="evidence-secondary-grid">

              <div className="evidence-secondary-card">

                <span>
                  Anomaly Rate
                </span>

                <strong>
                  {priority.anomaly_rate.toFixed(
                    2
                  )}
                  %
                </strong>

              </div>


              <div className="evidence-secondary-card">

                <span>
                  Model Agreement Days
                </span>

                <strong>
                  {priority.model_agreement_days}
                </strong>

              </div>


              <div className="evidence-secondary-card">

                <span>
                  Recent Anomaly Days
                </span>

                <strong>
                  {priority.recent_anomaly_days}
                </strong>

              </div>


              <div className="evidence-secondary-card">

                <span>
                  Maximum Event Risk
                </span>

                <strong>
                  {priority.max_event_risk_score.toFixed(
                    2
                  )}
                </strong>

              </div>

            </div>


            {/* INTERPRETATION */}

            <div className="evidence-interpretation">

              <div className="evidence-interpretation-icon">
                <ShieldAlert
                  size={20}
                />
              </div>

              <div>

                <strong>
                  Why this meter is prioritized
                </strong>

                <p>

                  GridGuard assigns this meter a{" "}
                  <strong>
                    {priorityLevel}
                  </strong>{" "}
                  investigation priority based on
                  the combined strength, persistence,
                  frequency, severity, recent activity,
                  and agreement of abnormal-consumption
                  signals.

                </p>

              </div>

            </div>


            {/* DISCLAIMER */}

            <div className="priority-disclaimer">

              <ShieldAlert
                size={18}
              />

              <span>

                The Investigation Priority Score
                indicates the strength of abnormal-
                consumption evidence and helps
                prioritize field review. It is not a
                probability of theft and does not
                confirm electricity theft.

              </span>

            </div>

          </>

        ) : (

          <div className="empty-priority-state">

            <ShieldAlert
              size={28}
            />

            <strong>
              Investigation evidence unavailable
            </strong>

            <span>
              {priorityError ||
                "No investigation-priority evidence is available for this meter."}
            </span>

          </div>

        )}

      </div>


      {/* ===================================================
          CONSUMPTION HISTORY
      =================================================== */}

      <div className="panel">

        <div className="panel-header">

          <div>

            <h3>
              Consumption History
            </h3>

            <p>
              Historical daily consumption
              for {meterId}
            </p>

          </div>

        </div>


        <div className="details-chart">

          {consumption.length > 0 ? (

            <ResponsiveContainer
              width="100%"
              height="100%"
            >

              <AreaChart
                data={consumption}
                margin={{
                  top: 10,
                  right: 20,
                  left: 5,
                  bottom: 10,
                }}
              >

                <CartesianGrid
                  strokeDasharray="3 3"
                />

                <XAxis
                  dataKey="date"
                  tick={{
                    fontSize: 11,
                  }}
                  tickFormatter={
                    formatAxisDate
                  }
                  minTickGap={35}
                />

                <YAxis
                  tick={{
                    fontSize: 11,
                  }}
                  tickFormatter={(value) =>
                    Number(value).toFixed(1)
                  }
                />

                <Tooltip
                  labelFormatter={
                    formatTooltipDate
                  }
                  formatter={(value) => [
                    `${Number(
                      value
                    ).toFixed(3)} kWh`,
                    "Consumption",
                  ]}
                  contentStyle={{
                    borderRadius:
                      "10px",
                    border:
                      "1px solid #e2e8f0",
                    boxShadow:
                      "0 4px 15px rgba(0,0,0,0.08)",
                  }}
                />

                <Area
                  type="monotone"
                  dataKey="consumption"
                  name="Daily Consumption"
                  stroke="#2563eb"
                  fill="#2563eb"
                  fillOpacity={0.12}
                  strokeWidth={2.5}
                  activeDot={{
                    r: 5,
                  }}
                />

              </AreaChart>

            </ResponsiveContainer>

          ) : (

            <div className="empty-chart">

              <Activity
                size={28}
              />

              <span>
                No consumption history
                available for this meter.
              </span>

            </div>

          )}

        </div>

      </div>


      {/* ===================================================
          CONSUMPTION VS BASELINE
      =================================================== */}

      <div className="panel">

        <div className="panel-header">

          <div>

            <h3>
              Consumption vs Baseline
            </h3>

            <p>
              Compare observed consumption
              against historical behavior
            </p>

          </div>

        </div>


        <div className="details-chart">

          {consumption.length > 0 ? (

            <ResponsiveContainer
              width="100%"
              height="100%"
            >

              <LineChart
                data={consumption}
                margin={{
                  top: 10,
                  right: 20,
                  left: 5,
                  bottom: 10,
                }}
              >

                <CartesianGrid
                  strokeDasharray="3 3"
                />

                <XAxis
                  dataKey="date"
                  tick={{
                    fontSize: 11,
                  }}
                  tickFormatter={
                    formatAxisDate
                  }
                  minTickGap={35}
                />

                <YAxis
                  tick={{
                    fontSize: 11,
                  }}
                  tickFormatter={(value) =>
                    Number(value).toFixed(1)
                  }
                />

                <Tooltip
                  labelFormatter={
                    formatTooltipDate
                  }
                  formatter={(
                    value,
                    name
                  ) => [
                    value == null
                      ? "—"
                      : `${Number(
                          value
                        ).toFixed(3)} kWh`,
                    name,
                  ]}
                  contentStyle={{
                    borderRadius:
                      "10px",
                    border:
                      "1px solid #e2e8f0",
                    boxShadow:
                      "0 4px 15px rgba(0,0,0,0.08)",
                  }}
                />

                <Legend />

                {/* OBSERVED */}

                <Line
                  type="monotone"
                  dataKey="consumption"
                  name="Actual Consumption"
                  stroke="#2563eb"
                  strokeWidth={2.5}
                  dot={false}
                  activeDot={{
                    r: 5,
                  }}
                />

                {/* BASELINE */}

                <Line
                  type="monotone"
                  dataKey="baseline"
                  name="7-Day Baseline"
                  stroke="#64748b"
                  strokeWidth={2}
                  strokeDasharray="6 4"
                  dot={false}
                  connectNulls
                />

              </LineChart>

            </ResponsiveContainer>

          ) : (

            <div className="empty-chart">

              <TrendingUp
                size={28}
              />

              <span>
                No baseline comparison
                available.
              </span>

            </div>

          )}

        </div>

      </div>


      {/* ===================================================
          INVESTIGATION NOTICE
      =================================================== */}

      <div className="investigation-notice">

        <ShieldAlert
          size={23}
        />

        <div>

          <strong>
            Investigation Recommendation
          </strong>

          <p>
            This meter's analytical indicators
            should be reviewed together with
            operational and field information.
            An anomaly or high investigation-priority
            score alone should not be treated as
            proof of electricity theft.
          </p>

        </div>

      </div>

    </div>
  );
}

export default MeterDetails;
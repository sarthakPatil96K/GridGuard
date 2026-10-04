import { useEffect, useMemo, useState } from "react";

import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  ReferenceLine,
} from "recharts";

import {
  getTrends,
  getRiskDistribution,
  getAnomalyTypes,
  getPriorities,
} from "../services/api";

/* =========================================================
   COLORS
========================================================= */

const RISK_COLORS = {
  LOW: "#22c55e",
  MEDIUM: "#f59e0b",
  HIGH: "#f97316",
  CRITICAL: "#ef4444",
};

const PRIORITY_COLORS = {
  "VERY HIGH": "#b91c1c",
  HIGH: "#ea580c",
  MEDIUM: "#d97706",
  LOW: "#16a34a",
  NORMAL: "#64748b",
};

const CHART_COLORS = {
  primary: "#2563eb",
  secondary: "#64748b",
  success: "#22c55e",
  warning: "#f59e0b",
  danger: "#ef4444",
};

/* =========================================================
   RESPONSE HELPERS
========================================================= */

function normalizeRows(response) {
  if (Array.isArray(response)) return response;

  if (Array.isArray(response?.data)) {
    return response.data;
  }

  if (Array.isArray(response?.data?.data)) {
    return response.data.data;
  }

  if (Array.isArray(response?.results)) {
    return response.results;
  }

  if (Array.isArray(response?.priorities)) {
    return response.priorities;
  }

  return [];
}

/* =========================================================
   CUSTOM TOOLTIP
========================================================= */

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload || payload.length === 0) {
    return null;
  }

  return (
    <div className="custom-chart-tooltip">
      <div className="tooltip-label">{label}</div>

      {payload.map((item, index) => (
        <div className="tooltip-row" key={index}>
          <span className="tooltip-name">
            <span
              className="tooltip-dot"
              style={{
                background: item.color,
              }}
            />

            {item.name}
          </span>

          <strong>
            {typeof item.value === "number"
              ? item.value.toLocaleString("en-IN", {
                  maximumFractionDigits: 2,
                })
              : item.value}
          </strong>
        </div>
      ))}
    </div>
  );
}

/* =========================================================
   DATE FORMATTER
========================================================= */

function formatDate(value) {
  if (!value) {
    return "";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleDateString("en-IN", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  });
}

/* =========================================================
   RISK COLOR
========================================================= */

function getRiskColor(level) {
  const normalized = String(level || "").toUpperCase();

  return RISK_COLORS[normalized] || "#94a3b8";
}

/* =========================================================
   PRIORITY COLOR
========================================================= */

function getPriorityColor(level) {
  const normalized = String(level || "").toUpperCase();

  return PRIORITY_COLORS[normalized] || "#94a3b8";
}

/* =========================================================
   PRIORITY LEVEL
========================================================= */

function normalizePriorityLevel(item) {
  const value = String(
    item?.priority_level ||
      item?.investigation_priority_level ||
      item?.level ||
      ""
  ).toUpperCase();

  if (
    [
      "VERY HIGH",
      "HIGH",
      "MEDIUM",
      "LOW",
      "NORMAL",
    ].includes(value)
  ) {
    return value;
  }

  return "NORMAL";
}

/* =========================================================
   ANALYTICS
========================================================= */

function Analytics() {
  const [trends, setTrends] = useState([]);
  const [riskDistribution, setRiskDistribution] = useState([]);
  const [anomalyTypes, setAnomalyTypes] = useState([]);
  const [priorities, setPriorities] = useState([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  /* =======================================================
     LOAD ANALYTICS
  ======================================================= */

  useEffect(() => {
    async function loadAnalytics() {
      try {
        setLoading(true);
        setError("");

        const [
          trendRes,
          riskRes,
          anomalyRes,
          priorityRes,
        ] = await Promise.all([
          getTrends(),
          getRiskDistribution(),
          getAnomalyTypes(),
          getPriorities(),
        ]);

        setTrends(normalizeRows(trendRes));

        setRiskDistribution(normalizeRows(riskRes));

        setAnomalyTypes(normalizeRows(anomalyRes));

        setPriorities(normalizeRows(priorityRes));
      } catch (error) {
        console.error("Analytics loading error:", error);

        setError(
          "Unable to load analytics data. Make sure the FastAPI server is running."
        );
      } finally {
        setLoading(false);
      }
    }

    loadAnalytics();
  }, []);

  /* =======================================================
     MODEL PERFORMANCE
  ======================================================= */

  const modelPerformance = [
    {
      model: "Contextual",
      precision: 0.0701,
      recall: 0.9167,
      f1: 0.1302,
      accuracy: 0.9766,
    },
    {
      model: "Isolation Forest",
      precision: 0.0704,
      recall: 0.4167,
      f1: 0.1205,
      accuracy: 0.9884,
    },
    {
      model: "GridGuard Ensemble",
      precision: 0.1410,
      recall: 0.9167,
      f1: 0.2444,
      accuracy: 0.9892,
    },
  ];

  /* =======================================================
     THRESHOLD DATA
  ======================================================= */

  const thresholdData = [
    {
      threshold: 0.65,
      tp: 11,
      fp: 155,
    },
    {
      threshold: 0.70,
      tp: 7,
      fp: 67,
    },
    {
      threshold: 0.75,
      tp: 5,
      fp: 21,
    },
    {
      threshold: 0.80,
      tp: 5,
      fp: 9,
    },
    {
      threshold: 0.85,
      tp: 5,
      fp: 4,
    },
  ];

  /* =======================================================
     SCALABILITY DATA
  ======================================================= */

  const scalabilityData = [
    {
      records: "100K",
      pandas: 0.44,
      spark: 6.76,
    },
    {
      records: "500K",
      pandas: 1.46,
      spark: 11.0,
    },
    {
      records: "1M",
      pandas: 3.0,
      spark: 21.17,
    },
    {
      records: "5M",
      pandas: 14.34,
      spark: 47.13,
    },
    {
      records: "10M",
      pandas: 34.39,
      spark: 47.43,
    },
  ];

  /* =======================================================
     PRIORITY SUMMARY
  ======================================================= */

  const prioritySummary = useMemo(() => {
    const summary = {
      "VERY HIGH": 0,
      HIGH: 0,
      MEDIUM: 0,
      LOW: 0,
      NORMAL: 0,
    };

    priorities.forEach((item) => {
      const level = normalizePriorityLevel(item);

      if (summary[level] !== undefined) {
        summary[level] += 1;
      }
    });

    return summary;
  }, [priorities]);

  /* =======================================================
     PRIORITY CHART DATA
  ======================================================= */

  const priorityChartData = useMemo(() => {
    return [
      {
        priority_level: "VERY HIGH",
        count: prioritySummary["VERY HIGH"],
      },
      {
        priority_level: "HIGH",
        count: prioritySummary.HIGH,
      },
      {
        priority_level: "MEDIUM",
        count: prioritySummary.MEDIUM,
      },
      {
        priority_level: "LOW",
        count: prioritySummary.LOW,
      },
      {
        priority_level: "NORMAL",
        count: prioritySummary.NORMAL,
      },
    ].filter((item) => item.count > 0);
  }, [prioritySummary]);

  /* =======================================================
     PRIORITY METRICS
  ======================================================= */

  const averagePriorityScore = useMemo(() => {
    const scores = priorities
      .map((item) =>
        Number(
          item?.investigation_priority_score ??
            item?.priority_score ??
            0
        )
      )
      .filter((score) => Number.isFinite(score));

    if (scores.length === 0) {
      return 0;
    }

    return (
      scores.reduce((sum, score) => sum + score, 0) /
      scores.length
    );
  }, [priorities]);

  const averageAgreement = useMemo(() => {
    const values = priorities
      .map((item) => Number(item?.agreement_rate ?? 0))
      .filter((value) => Number.isFinite(value));

    if (values.length === 0) {
      return 0;
    }

    return (
      values.reduce((sum, value) => sum + value, 0) /
      values.length
    );
  }, [priorities]);

  const highPriorityMeters =
    prioritySummary["VERY HIGH"] +
    prioritySummary.HIGH;

  /* =======================================================
     RISK TOTAL
  ======================================================= */

  const totalRiskMeters = riskDistribution.reduce(
    (sum, item) => sum + Number(item.count || 0),
    0
  );

  /* =======================================================
     LOADING
  ======================================================= */

  if (loading) {
    return (
      <div className="loading-state">
        Loading analytics...
      </div>
    );
  }

  /* =======================================================
     PAGE
  ======================================================= */

  return (
    <div className="analytics-page">

      {/* ===================================================
          HEADER
      =================================================== */}

      <div className="page-header">
        <div>
          <h1>Analytics</h1>

          <p>
            Big Data processing, anomaly detection,
            evidence correlation and investigation
            priority analysis.
          </p>
        </div>
      </div>

      {error && (
        <div className="error-state">
          {error}
        </div>
      )}

      {/* ===================================================
          INVESTIGATION PRIORITY OVERVIEW
      =================================================== */}

      <section className="analytics-section">

        <div className="section-header">
          <div>
            <h2>Investigation Priority Overview</h2>

            <p>
              Meter-level prioritization based on
              persistence, frequency, severity and
              agreement between analytical signals.
            </p>
          </div>
        </div>

        <div className="priority-overview-grid">

          <div className="priority-overview-card">
            <span className="priority-card-label">
              Very High Priority
            </span>

            <strong className="priority-number very-high">
              {prioritySummary["VERY HIGH"]}
            </strong>

            <small>
              Requires earliest review
            </small>
          </div>

          <div className="priority-overview-card">
            <span className="priority-card-label">
              High Priority
            </span>

            <strong className="priority-number high">
              {prioritySummary.HIGH}
            </strong>

            <small>
              Strong abnormal-consumption evidence
            </small>
          </div>

          <div className="priority-overview-card">
            <span className="priority-card-label">
              Medium Priority
            </span>

            <strong className="priority-number medium">
              {prioritySummary.MEDIUM}
            </strong>

            <small>
              Requires contextual review
            </small>
          </div>

          <div className="priority-overview-card">
            <span className="priority-card-label">
              Average Priority Score
            </span>

            <strong className="priority-number">
              {averagePriorityScore.toFixed(1)}
            </strong>

            <small>
              Across analyzed meters
            </small>
          </div>

          <div className="priority-overview-card">
            <span className="priority-card-label">
              Model Agreement
            </span>

            <strong className="priority-number">
              {averageAgreement.toFixed(1)}%
            </strong>

            <small>
              Average agreement rate
            </small>
          </div>

          <div className="priority-overview-card">
            <span className="priority-card-label">
              Elevated Priority
            </span>

            <strong className="priority-number high">
              {highPriorityMeters}
            </strong>

            <small>
              Very High + High meters
            </small>
          </div>

        </div>

        <div className="analytics-note">
          <strong>How to interpret priority:</strong>

          <p>
            Investigation priority represents the strength
            and persistence of abnormal consumption evidence.
            It is an operational ranking for review and is
            not a probability of electricity theft or proof
            of unauthorized activity.
          </p>
        </div>

      </section>

      {/* ===================================================
          MODEL PERFORMANCE
      =================================================== */}

      <section className="analytics-section">

        <div className="section-header">
          <div>
            <h2>Model Performance</h2>

            <p>
              Comparison of GridGuard anomaly-detection
              approaches.
            </p>
          </div>
        </div>

        <div className="performance-grid">

          {modelPerformance.map((model) => (
            <div
              className="performance-card"
              key={model.model}
            >
              <h3>{model.model}</h3>

              <div className="metric-row">
                <span>Precision</span>
                <strong>
                  {(model.precision * 100).toFixed(2)}%
                </strong>
              </div>

              <div className="metric-row">
                <span>Recall</span>
                <strong>
                  {(model.recall * 100).toFixed(2)}%
                </strong>
              </div>

              <div className="metric-row">
                <span>F1 Score</span>
                <strong>
                  {(model.f1 * 100).toFixed(2)}%
                </strong>
              </div>

              <div className="metric-row">
                <span>Accuracy</span>
                <strong>
                  {(model.accuracy * 100).toFixed(2)}%
                </strong>
              </div>
            </div>
          ))}

        </div>

        <div className="analytics-note">
          <strong>Model interpretation:</strong>

          <p>
            These metrics evaluate the ability of the
            analytical methods to identify injected or
            validated abnormal patterns. They should not
            be interpreted as direct measurements of
            confirmed electricity theft.
          </p>
        </div>

      </section>

      {/* ===================================================
          TREND + RISK
      =================================================== */}

      <div className="analytics-grid">

        {/* =================================================
            CONSUMPTION TREND
        ================================================= */}

        <section className="analytics-panel">

          <div className="panel-header">
            <div>
              <h2>Consumption Trend</h2>

              <span>
                Daily electricity consumption
              </span>
            </div>
          </div>

          <div className="chart-container">

            {trends.length > 0 ? (
              <ResponsiveContainer
                width="100%"
                height={330}
              >
                <LineChart
                  data={trends}
                  margin={{
                    top: 15,
                    right: 20,
                    left: 5,
                    bottom: 10,
                  }}
                >
                  <CartesianGrid
                    vertical={false}
                    stroke="#e2e8f0"
                    strokeDasharray="4 5"
                  />

                  <XAxis
                    dataKey="date"
                    tick={{
                      fontSize: 11,
                      fill: "#64748b",
                    }}
                    axisLine={false}
                    tickLine={false}
                    tickFormatter={formatDate}
                    minTickGap={40}
                  />

                  <YAxis
                    tick={{
                      fontSize: 11,
                      fill: "#64748b",
                    }}
                    axisLine={false}
                    tickLine={false}
                    width={55}
                  />

                  <Tooltip
                    content={<ChartTooltip />}
                  />

                  <Line
                    type="monotone"
                    dataKey="daily_consumption"
                    name="Daily Consumption"
                    stroke={CHART_COLORS.primary}
                    strokeWidth={2.5}
                    dot={false}
                    activeDot={{
                      r: 5,
                      strokeWidth: 3,
                      fill: "#ffffff",
                    }}
                  />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <div className="empty-chart">
                No trend data available
              </div>
            )}

          </div>
        </section>

        {/* =================================================
            ANOMALY RISK DISTRIBUTION
        ================================================= */}

        <section className="analytics-panel">

          <div className="panel-header">
            <div>
              <h2>Anomaly Risk Distribution</h2>

              <span>
                Current analytical risk levels
              </span>
            </div>
          </div>

          <div className="chart-container risk-chart">

            {riskDistribution.length > 0 ? (
              <ResponsiveContainer
                width="100%"
                height={330}
              >
                <PieChart>

                  <Pie
                    data={riskDistribution}
                    dataKey="count"
                    nameKey="risk_level"
                    cx="50%"
                    cy="50%"
                    innerRadius={72}
                    outerRadius={112}
                    paddingAngle={3}
                    cornerRadius={5}
                    stroke="#ffffff"
                    strokeWidth={3}
                  >

                    {riskDistribution.map(
                      (item, index) => (
                        <Cell
                          key={`risk-${index}`}
                          fill={getRiskColor(
                            item.risk_level
                          )}
                        />
                      )
                    )}

                  </Pie>

                  <Tooltip
                    formatter={(value, name) => [
                      `${Number(
                        value
                      ).toLocaleString("en-IN")} meters`,
                      name,
                    ]}
                  />

                  <Legend
                    verticalAlign="bottom"
                    height={30}
                  />

                  <text
                    x="50%"
                    y="47%"
                    textAnchor="middle"
                    dominantBaseline="middle"
                    fill="#0f172a"
                    fontSize={25}
                    fontWeight={700}
                  >
                    {totalRiskMeters.toLocaleString(
                      "en-IN"
                    )}
                  </text>

                  <text
                    x="50%"
                    y="56%"
                    textAnchor="middle"
                    dominantBaseline="middle"
                    fill="#64748b"
                    fontSize={11}
                  >
                    Total Meters
                  </text>

                </PieChart>
              </ResponsiveContainer>
            ) : (
              <div className="empty-chart">
                No risk data available
              </div>
            )}

          </div>
        </section>

      </div>

      {/* ===================================================
          PRIORITY DISTRIBUTION
      =================================================== */}

      <section className="analytics-panel full-width">

        <div className="panel-header">
          <div>
            <h2>Investigation Priority Distribution</h2>

            <span>
              Meter-level operational prioritization
            </span>
          </div>
        </div>

        <div className="chart-container">

          {priorityChartData.length > 0 ? (
            <ResponsiveContainer
              width="100%"
              height={330}
            >
              <BarChart
                data={priorityChartData}
                margin={{
                  top: 15,
                  right: 20,
                  left: 5,
                  bottom: 10,
                }}
              >

                <CartesianGrid
                  vertical={false}
                  stroke="#e2e8f0"
                  strokeDasharray="4 5"
                />

                <XAxis
                  dataKey="priority_level"
                  tick={{
                    fontSize: 11,
                    fill: "#64748b",
                  }}
                  axisLine={false}
                  tickLine={false}
                />

                <YAxis
                  tick={{
                    fontSize: 11,
                    fill: "#64748b",
                  }}
                  axisLine={false}
                  tickLine={false}
                  allowDecimals={false}
                />

                <Tooltip
                  formatter={(value) => [
                    `${value} meters`,
                    "Meters",
                  ]}
                />

                <Bar
                  dataKey="count"
                  name="Meters"
                  radius={[7, 7, 0, 0]}
                  maxBarSize={70}
                >
                  {priorityChartData.map(
                    (item, index) => (
                      <Cell
                        key={`priority-${index}`}
                        fill={getPriorityColor(
                          item.priority_level
                        )}
                      />
                    )
                  )}
                </Bar>

              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="empty-chart">
              No investigation-priority data available
            </div>
          )}

        </div>

        <div className="analytics-note">
          <strong>Priority construction:</strong>

          <p>
            GridGuard combines multiple analytical signals
            such as anomaly persistence, anomaly frequency,
            severity, recent activity and model agreement
            to rank meters for investigation.
          </p>
        </div>

      </section>

      {/* ===================================================
          ANOMALY TYPES
      =================================================== */}

      <section className="analytics-panel full-width">

        <div className="panel-header">
          <div>
            <h2>Anomaly Type Distribution</h2>

            <span>
              Detected abnormal-consumption categories
            </span>
          </div>
        </div>

        <div className="chart-container">

          {anomalyTypes.length > 0 ? (
            <ResponsiveContainer
              width="100%"
              height={320}
            >
              <BarChart
                data={anomalyTypes}
                margin={{
                  top: 15,
                  right: 20,
                  left: 5,
                  bottom: 20,
                }}
              >

                <CartesianGrid
                  vertical={false}
                  stroke="#e2e8f0"
                  strokeDasharray="4 5"
                />

                <XAxis
                  dataKey="anomaly_type"
                  tick={{
                    fontSize: 11,
                    fill: "#64748b",
                  }}
                  axisLine={false}
                  tickLine={false}
                  tickFormatter={(value) =>
                    String(value)
                      .replaceAll("_", " ")
                      .replace(/\b\w/g, (char) =>
                        char.toUpperCase()
                      )
                  }
                />

                <YAxis
                  tick={{
                    fontSize: 11,
                    fill: "#64748b",
                  }}
                  axisLine={false}
                  tickLine={false}
                  allowDecimals={false}
                />

                <Tooltip
                  cursor={{
                    fill: "#f8fafc",
                  }}
                  formatter={(value) => [
                    `${value} anomalies`,
                    "Detected",
                  ]}
                />

                <Bar
                  dataKey="count"
                  name="Anomalies"
                  fill={CHART_COLORS.primary}
                  radius={[7, 7, 0, 0]}
                  maxBarSize={70}
                  animationDuration={800}
                />

              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div className="empty-chart">
              No anomaly type data available
            </div>
          )}

        </div>
      </section>

      {/* ===================================================
          THRESHOLD ANALYSIS
      =================================================== */}

      <section className="analytics-panel full-width">

        <div className="panel-header">
          <div>
            <h2>Anomaly Detection Threshold Analysis</h2>

            <span>
              Effect of analytical threshold on detection
              and false-positive behavior
            </span>
          </div>
        </div>

        <div className="chart-container">

          <ResponsiveContainer
            width="100%"
            height={320}
          >
            <LineChart
              data={thresholdData}
              margin={{
                top: 15,
                right: 20,
                left: 5,
                bottom: 10,
              }}
            >

              <CartesianGrid
                vertical={false}
                stroke="#e2e8f0"
                strokeDasharray="4 5"
              />

              <XAxis
                dataKey="threshold"
                tick={{
                  fontSize: 11,
                  fill: "#64748b",
                }}
                axisLine={false}
                tickLine={false}
                tickFormatter={(value) =>
                  value.toFixed(2)
                }
              />

              <YAxis
                tick={{
                  fontSize: 11,
                  fill: "#64748b",
                }}
                axisLine={false}
                tickLine={false}
                allowDecimals={false}
              />

              <Tooltip
                formatter={(value, name) => [
                  value,
                  name,
                ]}
              />

              <Legend />

              <ReferenceLine
                x={0.80}
                stroke="#94a3b8"
                strokeDasharray="5 5"
                label={{
                  value: "Reference threshold",
                  position: "top",
                  fill: "#64748b",
                  fontSize: 11,
                }}
              />

              <Line
                type="monotone"
                dataKey="tp"
                name="True Positives"
                stroke={CHART_COLORS.success}
                strokeWidth={3}
                dot={{
                  r: 4,
                  fill: CHART_COLORS.success,
                  strokeWidth: 2,
                }}
                activeDot={{
                  r: 6,
                }}
              />

              <Line
                type="monotone"
                dataKey="fp"
                name="False Positives"
                stroke={CHART_COLORS.danger}
                strokeWidth={3}
                dot={{
                  r: 4,
                  fill: CHART_COLORS.danger,
                  strokeWidth: 2,
                }}
                activeDot={{
                  r: 6,
                }}
              />

            </LineChart>
          </ResponsiveContainer>

        </div>

        <div className="analytics-note">
          <strong>
            Threshold selection: 0.642
          </strong>

          <p>
            The selected analytical threshold was chosen
            to retain strong anomaly recall while controlling
            false-positive behavior. The threshold identifies
            unusual consumption patterns; it does not
            establish electricity theft.
          </p>
        </div>

      </section>

      {/* ===================================================
          SCALABILITY
      =================================================== */}

      <section className="analytics-panel full-width">

        <div className="panel-header">
          <div>
            <h2>Big Data Scalability</h2>

            <span>
              Pandas vs Apache Spark processing time
            </span>
          </div>
        </div>

        <div className="chart-container">

          <ResponsiveContainer
            width="100%"
            height={340}
          >
            <BarChart
              data={scalabilityData}
              margin={{
                top: 15,
                right: 20,
                left: 10,
                bottom: 10,
              }}
              barGap={6}
            >

              <CartesianGrid
                vertical={false}
                stroke="#e2e8f0"
                strokeDasharray="4 5"
              />

              <XAxis
                dataKey="records"
                tick={{
                  fontSize: 11,
                  fill: "#64748b",
                }}
                axisLine={false}
                tickLine={false}
              />

              <YAxis
                tick={{
                  fontSize: 11,
                  fill: "#64748b",
                }}
                axisLine={false}
                tickLine={false}
                label={{
                  value: "Time (seconds)",
                  angle: -90,
                  position: "insideLeft",
                  fill: "#64748b",
                  fontSize: 11,
                }}
              />

              <Tooltip
                cursor={{
                  fill: "#f8fafc",
                }}
                formatter={(value, name) => [
                  `${Number(value).toFixed(2)} sec`,
                  name,
                ]}
              />

              <Legend />

              <Bar
                dataKey="pandas"
                name="Pandas"
                fill="#94a3b8"
                radius={[6, 6, 0, 0]}
                maxBarSize={42}
              />

              <Bar
                dataKey="spark"
                name="Apache Spark"
                fill={CHART_COLORS.primary}
                radius={[6, 6, 0, 0]}
                maxBarSize={42}
              />

            </BarChart>
          </ResponsiveContainer>

        </div>

        <div className="analytics-note">
          <strong>
            Full dataset: 167.9M records
          </strong>

          <p>
            Apache Spark was used for large-scale data
            processing and feature computation. The
            scalability experiment demonstrates why
            distributed-style processing is appropriate
            for GridGuard's smart-meter workload.
          </p>
        </div>

      </section>

      {/* ===================================================
          FINAL DISCLAIMER
      =================================================== */}

      <div className="analytics-disclaimer">

        <strong>Important:</strong>

        <p>
          GridGuard identifies unusual electricity-consumption
          patterns and ranks meters for investigation. A high
          anomaly score or investigation-priority score indicates
          stronger analytical evidence of abnormal behavior. It
          does not establish electricity theft, unauthorized
          activity, or a probability of theft. Final decisions
          require additional operational evidence and field
          validation.
        </p>

      </div>

    </div>
  );
}

export default Analytics;
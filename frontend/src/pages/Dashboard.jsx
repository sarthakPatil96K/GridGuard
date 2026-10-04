import { useEffect, useState } from "react";

import {
  Activity,
  Gauge,
  TriangleAlert,
  ShieldAlert,
  Zap,
} from "lucide-react";

import {
  getDashboardSummary,
  getTrends,
  getRiskDistribution,
  getPriorities,
} from "../services/api";

import KPICard from "../components/KPICard";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Legend,
} from "recharts";


/* =========================================================
   HELPER FUNCTIONS
========================================================= */

function extractArray(response) {
  if (Array.isArray(response)) {
    return response;
  }

  if (Array.isArray(response?.data)) {
    return response.data;
  }

  if (Array.isArray(response?.results)) {
    return response.results;
  }

  if (Array.isArray(response?.trends)) {
    return response.trends;
  }

  if (Array.isArray(response?.risk_distribution)) {
    return response.risk_distribution;
  }

  return [];
}


/* =========================================================
   NORMALIZE TREND DATA
========================================================= */

function normalizeTrendData(response) {
  const rows = extractArray(response);

  return rows
    .map((item) => {
      const rawDate =
        item.date ??
        item.analysis_date ??
        item.Date ??
        item.datetime ??
        item.DateTime;

      const rawConsumption =
        item.daily_consumption ??
        item.avg_consumption ??
        item.consumption ??
        item.total_consumption ??
        item.value;

      return {
        date: rawDate,
        consumption:
          rawConsumption === null ||
          rawConsumption === undefined
            ? null
            : Number(rawConsumption),
      };
    })
    .filter(
      (item) =>
        item.date &&
        Number.isFinite(item.consumption)
    );
}


/* =========================================================
   NORMALIZE RISK DATA
========================================================= */

function normalizeRiskData(response) {
  const rows = extractArray(response);

  return rows
    .map((item) => ({
      risk_level:
        item.risk_level ??
        item.level ??
        item.risk ??
        "UNKNOWN",

      count: Number(
        item.count ??
        item.total ??
        item.value ??
        0
      ),
    }))
    .filter(
      (item) =>
        item.count > 0
    );
}


/* =========================================================
   NORMALIZE PRIORITY DATA
========================================================= */

function normalizePriorityData(response) {
  const rows = Array.isArray(response)
    ? response
    : Array.isArray(response?.data)
      ? response.data
      : Array.isArray(response?.results)
        ? response.results
        : [];

  return rows
    .map((item) => ({
      meter_id:
        item.meter_id ??
        item.meter ??
        "UNKNOWN",

      investigation_priority_score:
        Number(
          item.investigation_priority_score ??
          item.priority_score ??
          0
        ),

      priority_level:
        String(
          item.priority_level ??
          item.level ??
          "NORMAL"
        ).toUpperCase(),

      anomaly_days:
        Number(
          item.anomaly_days ?? 0
        ),

      high_risk_days:
        Number(
          item.high_risk_days ?? 0
        ),

      agreement_rate:
        Number(
          item.agreement_rate ?? 0
        ),

      persistence_score:
        Number(
          item.persistence_score ?? 0
        ),

      frequency_score:
        Number(
          item.frequency_score ?? 0
        ),

      severity_score:
        Number(
          item.severity_score ?? 0
        ),

      recent_activity_score:
        Number(
          item.recent_activity_score ?? 0
        ),
    }))
    .sort(
      (a, b) =>
        b.investigation_priority_score -
        a.investigation_priority_score
    );
}


/* =========================================================
   FORMAT LARGE NUMBERS
========================================================= */

function formatNumber(value) {
  if (
    value === null ||
    value === undefined
  ) {
    return "0";
  }

  const number = Number(value);

  if (Number.isNaN(number)) {
    return "0";
  }

  return number.toLocaleString("en-IN");
}


/* =========================================================
   FORMAT CHART DATES
========================================================= */

function formatChartDate(value) {
  if (!value) {
    return "";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
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
   FORMAT X-AXIS DATE
========================================================= */

function formatAxisDate(value) {
  if (!value) {
    return "";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
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


/* =========================================================
   DASHBOARD
========================================================= */

function Dashboard() {

  const [summary, setSummary] =
    useState(null);

  const [trends, setTrends] =
    useState([]);

  const [riskDistribution, setRiskDistribution] =
    useState([]);

  const [priorities, setPriorities] =
    useState([]);

  const [loading, setLoading] =
    useState(true);

  const [error, setError] =
    useState("");


  /* =======================================================
     LOAD DASHBOARD DATA
  ======================================================= */

  useEffect(() => {

    async function loadDashboard() {

      try {

        setLoading(true);

        setError("");


        const [
          summaryResponse,
          trendsResponse,
          riskResponse,
          prioritiesResponse,
        ] = await Promise.all([

          getDashboardSummary(),

          getTrends(),

          getRiskDistribution(),

          getPriorities(),

        ]);


        /* -------------------------------
           Summary
        -------------------------------- */

        setSummary(
          summaryResponse?.data ?? {}
        );


        /* -------------------------------
           Consumption Trend
        -------------------------------- */

        const normalizedTrends =
          normalizeTrendData(
            trendsResponse?.data
          );

        console.log(
          "GridGuard trend response:",
          trendsResponse?.data
        );

        console.log(
          "Normalized trend data:",
          normalizedTrends
        );

        setTrends(
          normalizedTrends
        );


        /* -------------------------------
           Risk Distribution
        -------------------------------- */

        const normalizedRisk =
          normalizeRiskData(
            riskResponse?.data
          );

        console.log(
          "GridGuard risk response:",
          riskResponse?.data
        );

        console.log(
          "Normalized risk data:",
          normalizedRisk
        );

        setRiskDistribution(
          normalizedRisk
        );


        /* -------------------------------
           Investigation Priority
        -------------------------------- */

        const normalizedPriorities =
          normalizePriorityData(
            prioritiesResponse?.data
          );

        console.log(
          "GridGuard priority response:",
          prioritiesResponse?.data
        );

        console.log(
          "Normalized priority data:",
          normalizedPriorities
        );

        setPriorities(
          normalizedPriorities
        );


      } catch (err) {

        console.error(
          "Dashboard loading error:",
          err
        );

        setError(
          "Unable to load dashboard data. Make sure the FastAPI server is running."
        );

      } finally {

        setLoading(false);

      }

    }


    loadDashboard();

  }, []);


  /* =======================================================
     LOADING
  ======================================================= */

  if (loading) {

    return (

      <div className="loading-state">

        <div className="loading-spinner">
        </div>

        <span>
          Loading GridGuard analytics...
        </span>

      </div>

    );

  }


  /* =======================================================
     ERROR
  ======================================================= */

  if (error) {

    return (

      <div className="error-state">

        <div className="error-icon">
          !
        </div>

        <h3>
          Dashboard unavailable
        </h3>

        <p>
          {error}
        </p>

      </div>

    );

  }


  /* =======================================================
     RISK TOTAL
  ======================================================= */

  const totalRiskMeters =
    riskDistribution.reduce(
      (total, item) =>
        total +
        Number(item.count || 0),
      0
    );


  /* =======================================================
     PRIORITY COUNTS
  ======================================================= */

  const veryHighPriorityCount =
    priorities.filter(
      (item) =>
        item.priority_level ===
        "VERY HIGH"
    ).length;


  const highPriorityCount =
    priorities.filter(
      (item) =>
        item.priority_level ===
        "HIGH"
    ).length;


  const mediumPriorityCount =
    priorities.filter(
      (item) =>
        item.priority_level ===
        "MEDIUM"
    ).length;


  const lowPriorityCount =
    priorities.filter(
      (item) =>
        item.priority_level ===
        "LOW"
    ).length;


  const highPriorityMeters =
    veryHighPriorityCount +
    highPriorityCount;


  /* =======================================================
     RISK COLORS
  ======================================================= */

  const riskColors = {

    LOW: "#22c55e",

    MEDIUM: "#f59e0b",

    HIGH: "#f97316",

    CRITICAL: "#ef4444",

    UNKNOWN: "#94a3b8",

  };


  /* =======================================================
     PRIORITY BADGE
  ======================================================= */

  function getPriorityClass(level) {

    switch (level) {

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


  /* =======================================================
     RENDER
  ======================================================= */

  return (

    <div className="dashboard-page">


      {/* ===================================================
          PAGE HEADER
      =================================================== */}

      <div className="page-heading">

        <div>

          <h2>
            Dashboard
          </h2>

          <p>
            Operational overview of electricity
            consumption, anomalies, and investigation
            priorities.
          </p>

        </div>


        <div className="live-indicator">

          <span>
          </span>

          Analytics Active

        </div>

      </div>


      {/* ===================================================
          KPI CARDS
      =================================================== */}

      <div className="kpi-grid">


        <KPICard
          title="Total Meters"
          value={formatNumber(
            summary?.total_meters
          )}
          subtitle="Active monitoring"
          icon={Gauge}
        />


        <KPICard
          title="Total Readings"
          value={formatNumber(
            summary?.total_readings
          )}
          subtitle="Processed measurements"
          icon={Activity}
        />


        <KPICard
          title="Detected Anomalies"
          value={formatNumber(
            summary?.total_anomalies
          )}
          subtitle="Requires analysis"
          icon={TriangleAlert}
        />


        <KPICard
          title="Investigation Priority"
          value={formatNumber(
            highPriorityMeters
          )}
          subtitle="High-priority meters"
          icon={ShieldAlert}
        />

      </div>


      {/* ===================================================
          CHART GRID
      =================================================== */}

      <div className="dashboard-grid">


        {/* =================================================
            CONSUMPTION TREND
        ================================================= */}

        <div className="panel large-panel">

          <div className="panel-header">

            <div>

              <h3>
                Consumption Trend
              </h3>

              <p>
                Daily electricity consumption
              </p>

            </div>

            <Zap size={20} />

          </div>


          <div className="chart-container">

            {trends.length > 0 ? (

              <ResponsiveContainer
                width="100%"
                height="100%"
              >

                <LineChart
                  data={trends}
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
                      formatChartDate
                    }

                    formatter={(value) => [
                      `${Number(value).toFixed(3)} kWh`,
                      "Consumption",
                    ]}

                    contentStyle={{
                      borderRadius: "10px",
                      border:
                        "1px solid #e2e8f0",
                      boxShadow:
                        "0 4px 15px rgba(0,0,0,0.08)",
                    }}

                  />


                  <Line
                    type="monotone"
                    dataKey="consumption"
                    name="Daily Consumption"
                    stroke="#2563eb"
                    strokeWidth={2.5}
                    dot={false}
                    activeDot={{
                      r: 5,
                    }}
                  />

                </LineChart>

              </ResponsiveContainer>

            ) : (

              <div className="empty-chart">

                <Activity size={28} />

                <span>
                  No consumption history available
                </span>

              </div>

            )}

          </div>

        </div>


        {/* =================================================
            ANOMALY RISK DISTRIBUTION
        ================================================= */}

        <div className="panel">

          <div className="panel-header">

            <div>

              <h3>
                Anomaly Risk Distribution
              </h3>

              <p>
                Current analytical anomaly-risk levels
              </p>

            </div>

          </div>


          <div className="chart-container risk-chart-container">

            {riskDistribution.length > 0 ? (

              <ResponsiveContainer
                width="100%"
                height="100%"
              >

                <PieChart>

                  <Pie
                    data={riskDistribution}
                    dataKey="count"
                    nameKey="risk_level"

                    cx="50%"
                    cy="45%"

                    innerRadius={65}
                    outerRadius={100}

                    paddingAngle={3}
                    cornerRadius={5}

                    labelLine={false}

                    label={({
                      name,
                      percent,
                    }) =>
                      `${name} ${(
                        percent * 100
                      ).toFixed(0)}%`
                    }
                  >

                    {riskDistribution.map(
                      (entry, index) => {

                        const level =
                          String(
                            entry.risk_level ||
                            "UNKNOWN"
                          ).toUpperCase();

                        return (

                          <Cell
                            key={`risk-${index}`}
                            fill={
                              riskColors[
                                level
                              ] ||
                              riskColors.UNKNOWN
                            }
                          />

                        );

                      }
                    )}

                  </Pie>


                  <Tooltip

                    formatter={(
                      value,
                      name
                    ) => [

                      formatNumber(value),

                      String(name).toUpperCase(),

                    ]}

                    contentStyle={{
                      borderRadius: "10px",
                      border:
                        "1px solid #e2e8f0",
                      boxShadow:
                        "0 4px 15px rgba(0,0,0,0.08)",
                    }}

                  />


                  <Legend
                    verticalAlign="bottom"
                    height={35}
                    iconType="circle"
                    formatter={(value) =>
                      String(
                        value
                      ).toUpperCase()
                    }
                  />


                  {/* CENTER TOTAL */}

                  <text
                    x="50%"
                    y="43%"
                    textAnchor="middle"
                    dominantBaseline="middle"
                    className="risk-total-label"
                  >
                    {formatNumber(
                      totalRiskMeters
                    )}
                  </text>


                  <text
                    x="50%"
                    y="51%"
                    textAnchor="middle"
                    dominantBaseline="middle"
                    className="risk-total-subtitle"
                  >
                    METERS
                  </text>

                </PieChart>

              </ResponsiveContainer>

            ) : (

              <div className="empty-chart">

                <ShieldAlert size={28} />

                <span>
                  No anomaly-risk distribution available
                </span>

              </div>

            )}

          </div>

        </div>


      </div>


      {/* ===================================================
          INVESTIGATION PRIORITY
      =================================================== */}

      <div className="panel priority-panel">

        <div className="panel-header">

          <div>

            <h3>
              Investigation Priority
            </h3>

            <p>
              Meters ranked by the strength, persistence,
              frequency, model agreement, and recent activity
              of abnormal consumption.
            </p>

          </div>

          <ShieldAlert size={20} />

        </div>


        {/* PRIORITY SUMMARY */}

        <div className="priority-summary">


          <div className="priority-summary-item">

            <span>
              Very High
            </span>

            <strong>
              {formatNumber(
                veryHighPriorityCount
              )}
            </strong>

          </div>


          <div className="priority-summary-item">

            <span>
              High
            </span>

            <strong>
              {formatNumber(
                highPriorityCount
              )}
            </strong>

          </div>


          <div className="priority-summary-item">

            <span>
              Medium
            </span>

            <strong>
              {formatNumber(
                mediumPriorityCount
              )}
            </strong>

          </div>


          <div className="priority-summary-item">

            <span>
              Low
            </span>

            <strong>
              {formatNumber(
                lowPriorityCount
              )}
            </strong>

          </div>


        </div>


        {/* PRIORITY TABLE */}

        {priorities.length > 0 ? (

          <div className="priority-table-wrapper">

            <table className="priority-table">

              <thead>

                <tr>

                  <th>
                    Rank
                  </th>

                  <th>
                    Meter
                  </th>

                  <th>
                    Priority Score
                  </th>

                  <th>
                    Level
                  </th>

                  <th>
                    Anomaly Days
                  </th>

                  <th>
                    High-Risk Days
                  </th>

                  <th>
                    Model Agreement
                  </th>

                </tr>

              </thead>


              <tbody>

                {priorities
                  .slice(0, 10)
                  .map(
                    (meter, index) => (

                      <tr
                        key={
                          meter.meter_id
                        }
                      >

                        <td>
                          #{index + 1}
                        </td>


                        <td className="meter-id-cell">

                          {meter.meter_id}

                        </td>


                        <td>

                          <strong>

                            {Number(
                              meter.investigation_priority_score
                            ).toFixed(2)}

                          </strong>

                          <span className="score-max">
                            {" "} / 100
                          </span>

                        </td>


                        <td>

                          <span
                            className={`priority-badge ${getPriorityClass(
                              meter.priority_level
                            )}`}
                          >

                            {
                              meter.priority_level
                            }

                          </span>

                        </td>


                        <td>

                          {formatNumber(
                            meter.anomaly_days
                          )}

                        </td>


                        <td>

                          {formatNumber(
                            meter.high_risk_days
                          )}

                        </td>


                        <td>

                          {Number(
                            meter.agreement_rate
                          ).toFixed(1)}

                          %

                        </td>

                      </tr>

                    )
                  )}

              </tbody>

            </table>

          </div>

        ) : (

          <div className="empty-priority-state">

            <ShieldAlert size={28} />

            <span>
              No investigation-priority data available.
            </span>

          </div>

        )}


        {/* PRIORITY DISCLAIMER */}

        <div className="priority-disclaimer">

          <ShieldAlert size={18} />

          <span>

            The Investigation Priority Score indicates
            the strength of abnormal-consumption evidence
            and helps prioritize field review. It is not a
            probability of theft and does not confirm theft.

          </span>

        </div>


      </div>


      {/* ===================================================
          INVESTIGATION NOTICE
      =================================================== */}

      <div className="information-panel">

        <ShieldAlert size={22} />

        <div>

          <strong>
            Investigation Guidance
          </strong>

          <p>

            GridGuard identifies statistically unusual
            consumption patterns and ranks meters for
            investigation. A high-priority score is an
            analytical flag, not proof or a probability
            of electricity theft.

          </p>

        </div>

      </div>


    </div>

  );

}


export default Dashboard;
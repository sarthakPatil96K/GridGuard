import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import {
  Search,
  RefreshCw,
  AlertTriangle,
  ShieldAlert,
  Activity,
  CheckCircle2,
  Eye,
  ChevronLeft,
  ChevronRight,
} from "lucide-react";

import {
  getAnomalies,
  getHighRiskAnomalies,
} from "../services/api";

import RiskBadge from "../components/RiskBadge";


/* =========================================================
   NORMALIZE API RESPONSE
========================================================= */

function normalizeResponse(response) {
  if (Array.isArray(response)) {
    return response;
  }

  if (Array.isArray(response?.data)) {
    return response.data;
  }

  if (Array.isArray(response?.data?.data)) {
    return response.data.data;
  }

  if (Array.isArray(response?.results)) {
    return response.results;
  }

  return [];
}


/* =========================================================
   FORMAT DATE
========================================================= */

function formatDate(value) {
  if (!value) {
    return "—";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return String(value);
  }

  return date.toLocaleDateString(
    "en-IN",
    {
      year: "numeric",
      month: "short",
      day: "numeric",
    }
  );
}


/* =========================================================
   FORMAT ANOMALY TYPE
========================================================= */

function formatAnomalyType(type) {
  if (!type) {
    return "Unknown";
  }

  return String(type)
    .replaceAll("_", " ")
    .replace(/\b\w/g, (letter) =>
      letter.toUpperCase()
    );
}


/* =========================================================
   GET ANALYTICAL RISK LEVEL
========================================================= */

function getRiskLevel(score) {
  if (score >= 80) {
    return "CRITICAL";
  }

  if (score >= 60) {
    return "HIGH";
  }

  if (score >= 30) {
    return "MEDIUM";
  }

  return "LOW";
}


/* =========================================================
   GET INVESTIGATION PRIORITY
========================================================= */

function getPriorityLevel(item) {
  const explicitLevel = String(
    item?.priority_level ||
    item?.investigation_priority_level ||
    ""
  ).toUpperCase();

  if (explicitLevel) {
    if (
      [
        "VERY HIGH",
        "HIGH",
        "MEDIUM",
        "LOW",
        "NORMAL",
      ].includes(explicitLevel)
    ) {
      return explicitLevel;
    }
  }

  /*
   * Existing anomaly records contain risk_score.
   *
   * This fallback maps the analytical risk level
   * to an investigation-priority label.
   *
   * It is only a UI fallback when a dedicated
   * priority value is not returned by the API.
   */

  const riskScore = Number(
    item?.risk_score ??
    item?.anomaly_score ??
    0
  );

  if (riskScore >= 80) {
    return "VERY HIGH";
  }

  if (riskScore >= 60) {
    return "HIGH";
  }

  if (riskScore >= 30) {
    return "MEDIUM";
  }

  return "NORMAL";
}


/* =========================================================
   PRIORITY CLASS
========================================================= */

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

    case "NORMAL":
      return "priority-normal";

    default:
      return "priority-normal";
  }
}


/* =========================================================
   PRIORITY BADGE
========================================================= */

function PriorityBadge({ level }) {
  return (
    <span
      className={`priority-badge ${getPriorityClass(
        level
      )}`}
    >
      {level}
    </span>
  );
}


/* =========================================================
   ANOMALIES PAGE
========================================================= */

function Anomalies() {
  const [anomalies, setAnomalies] =
    useState([]);

  const [highRisk, setHighRisk] =
    useState([]);

  const [search, setSearch] =
    useState("");

  const [riskFilter, setRiskFilter] =
    useState("ALL");

  const [priorityFilter, setPriorityFilter] =
    useState("ALL");

  const [typeFilter, setTypeFilter] =
    useState("ALL");

  const [loading, setLoading] =
    useState(true);

  const [error, setError] =
    useState("");

  const [currentPage, setCurrentPage] =
    useState(1);

  const itemsPerPage = 10;


  /* =======================================================
     LOAD DATA
  ======================================================= */

  const loadAnomalies = async () => {
    try {
      setLoading(true);
      setError("");

      const [
        allResponse,
        highRiskResponse,
      ] = await Promise.all([
        getAnomalies(),
        getHighRiskAnomalies(),
      ]);


      const allData =
        normalizeResponse(
          allResponse.data
        );


      const highRiskData =
        normalizeResponse(
          highRiskResponse.data
        );


      setAnomalies(allData);
      setHighRisk(highRiskData);

    } catch (err) {
      console.error(
        "Anomaly loading error:",
        err
      );

      setError(
        "Unable to load anomaly data. Make sure the FastAPI server is running."
      );

    } finally {
      setLoading(false);
    }
  };


  /* =======================================================
     INITIAL LOAD
  ======================================================= */

  useEffect(() => {
    loadAnomalies();
  }, []);


  /* =======================================================
     ANOMALY TYPES
  ======================================================= */

  const anomalyTypes = useMemo(() => {
    const types = anomalies
      .map(
        (item) =>
          item.anomaly_type
      )
      .filter(Boolean);

    return [
      ...new Set(types),
    ];
  }, [anomalies]);


  /* =======================================================
     FILTER DATA
  ======================================================= */

  const filteredAnomalies =
    useMemo(() => {
      const searchValue =
        search
          .toLowerCase()
          .trim();

      return anomalies.filter(
        (item) => {

          const meterId =
            String(
              item.meter_id || ""
            ).toLowerCase();

          const anomalyType =
            String(
              item.anomaly_type ||
                ""
            ).toLowerCase();


          const riskScore =
            Number(
              item.risk_score ??
              item.anomaly_score ??
              0
            );


          const riskLevel =
            String(
              item.risk_level ||
                getRiskLevel(
                  riskScore
                )
            ).toUpperCase();


          const priorityLevel =
            getPriorityLevel(
              item
            );


          /* ---------------------------------------------
             SEARCH
          --------------------------------------------- */

          const matchesSearch =
            meterId.includes(
              searchValue
            ) ||
            anomalyType.includes(
              searchValue
            );


          /* ---------------------------------------------
             RISK FILTER
          --------------------------------------------- */

          const matchesRisk =
            riskFilter === "ALL" ||
            riskLevel ===
              riskFilter;


          /* ---------------------------------------------
             PRIORITY FILTER
          --------------------------------------------- */

          const matchesPriority =
            priorityFilter ===
              "ALL" ||
            priorityLevel ===
              priorityFilter;


          /* ---------------------------------------------
             TYPE FILTER
          --------------------------------------------- */

          const matchesType =
            typeFilter === "ALL" ||
            item.anomaly_type ===
              typeFilter;


          return (
            matchesSearch &&
            matchesRisk &&
            matchesPriority &&
            matchesType
          );
        }
      );
    }, [
      anomalies,
      search,
      riskFilter,
      priorityFilter,
      typeFilter,
    ]);


  /* =======================================================
     PAGINATION
  ======================================================= */

  const totalPages =
    Math.max(
      1,
      Math.ceil(
        filteredAnomalies.length /
          itemsPerPage
      )
    );


  const safePage =
    Math.min(
      currentPage,
      totalPages
    );


  const paginatedAnomalies =
    filteredAnomalies.slice(
      (safePage - 1) *
        itemsPerPage,

      safePage *
        itemsPerPage
    );


  /* =======================================================
     RESET PAGE
  ======================================================= */

  const resetPage = () => {
    setCurrentPage(1);
  };


  /* =======================================================
     COUNTS
  ======================================================= */

  const veryHighCount =
    anomalies.filter(
      (item) =>
        getPriorityLevel(
          item
        ) === "VERY HIGH"
    ).length;


  const highPriorityCount =
    anomalies.filter(
      (item) =>
        getPriorityLevel(
          item
        ) === "HIGH"
    ).length;


  const mediumPriorityCount =
    anomalies.filter(
      (item) =>
        getPriorityLevel(
          item
        ) === "MEDIUM"
    ).length;


  const injectedCount =
    anomalies.filter(
      (item) =>
        item.ground_truth ===
        true
    ).length;


  /* =======================================================
     LOADING
  ======================================================= */

  if (loading) {
    return (
      <div className="loading-state">
        Loading anomaly analytics...
      </div>
    );
  }


  /* =======================================================
     RENDER
  ======================================================= */

  return (
    <div className="anomalies-page">


      {/* =================================================
          HEADER
      ================================================= */}

      <div className="page-heading">

        <div>

          <h2>
            Anomaly Investigation
          </h2>

          <p>
            Review abnormal electricity
            consumption patterns and
            prioritize meters for
            investigation.
          </p>

        </div>


        <button
          className="refresh-button"
          onClick={
            loadAnomalies
          }
          disabled={loading}
        >

          <RefreshCw
            size={17}
          />

          Refresh

        </button>

      </div>


      {/* =================================================
          ERROR
      ================================================= */}

      {error && (
        <div className="error-state">
          {error}
        </div>
      )}


      {/* =================================================
          KPI CARDS
      ================================================= */}

      <div className="anomaly-kpi-grid">


        {/* TOTAL */}

        <div className="anomaly-kpi">

          <div className="anomaly-kpi-icon blue">
            <Activity
              size={21}
            />
          </div>

          <div>

            <span>
              Total Detected
            </span>

            <strong>
              {anomalies.length}
            </strong>

          </div>

        </div>


        {/* VERY HIGH */}

        <div className="anomaly-kpi">

          <div className="anomaly-kpi-icon red">
            <ShieldAlert
              size={21}
            />
          </div>

          <div>

            <span>
              Very High Priority
            </span>

            <strong>
              {veryHighCount}
            </strong>

          </div>

        </div>


        {/* HIGH */}

        <div className="anomaly-kpi">

          <div className="anomaly-kpi-icon orange">
            <AlertTriangle
              size={21}
            />
          </div>

          <div>

            <span>
              High Priority
            </span>

            <strong>
              {highPriorityCount}
            </strong>

          </div>

        </div>


        {/* MEDIUM */}

        <div className="anomaly-kpi">

          <div className="anomaly-kpi-icon yellow">
            <Activity
              size={21}
            />
          </div>

          <div>

            <span>
              Medium Priority
            </span>

            <strong>
              {mediumPriorityCount}
            </strong>

          </div>

        </div>

      </div>


      {/* =================================================
          PRIORITY NOTICE
      ================================================= */}

      {highRisk.length > 0 && (

        <div className="high-risk-notice">

          <ShieldAlert
            size={21}
          />

          <div>

            <strong>
              {highRisk.length} elevated
              analytical flags require
              review
            </strong>

            <p>
              These records show stronger
              abnormal-consumption signals.
              They should be reviewed with
              additional operational evidence
              before any field action.
            </p>

          </div>

        </div>

      )}


      {/* =================================================
          FILTERS
      ================================================= */}

      <div className="filter-panel">


        {/* SEARCH */}

        <div className="meter-search">

          <Search size={18} />

          <input
            type="text"
            placeholder="Search meter or anomaly type..."
            value={search}
            onChange={(e) => {
              setSearch(
                e.target.value
              );

              resetPage();
            }}
          />

        </div>


        {/* RISK FILTER */}

        <select
          className="risk-filter"
          value={riskFilter}
          onChange={(e) => {

            setRiskFilter(
              e.target.value
            );

            resetPage();

          }}
        >

          <option value="ALL">
            All Analytical Risk
          </option>

          <option value="LOW">
            Low
          </option>

          <option value="MEDIUM">
            Medium
          </option>

          <option value="HIGH">
            High
          </option>

          <option value="CRITICAL">
            Critical
          </option>

        </select>


        {/* PRIORITY FILTER */}

        <select
          className="risk-filter"
          value={
            priorityFilter
          }
          onChange={(e) => {

            setPriorityFilter(
              e.target.value
            );

            resetPage();

          }}
        >

          <option value="ALL">
            All Investigation Priorities
          </option>

          <option value="VERY HIGH">
            Very High
          </option>

          <option value="HIGH">
            High
          </option>

          <option value="MEDIUM">
            Medium
          </option>

          <option value="LOW">
            Low
          </option>

          <option value="NORMAL">
            Normal
          </option>

        </select>


        {/* TYPE FILTER */}

        <select
          className="risk-filter"
          value={
            typeFilter
          }
          onChange={(e) => {

            setTypeFilter(
              e.target.value
            );

            resetPage();

          }}
        >

          <option value="ALL">
            All Anomaly Types
          </option>

          {anomalyTypes.map(
            (type) => (

              <option
                key={type}
                value={type}
              >
                {formatAnomalyType(
                  type
                )}
              </option>

            )
          )}

        </select>

      </div>


      {/* =================================================
          TABLE
      ================================================= */}

      <div className="table-panel">


        <div className="table-header">

          <div>

            <h3>
              Detected Anomalies
            </h3>

            <p>
              Showing{" "}
              {
                paginatedAnomalies.length
              }{" "}
              of{" "}
              {
                filteredAnomalies.length
              }{" "}
              records
            </p>

          </div>

        </div>


        <div className="table-wrapper">

          <table>

            <thead>

              <tr>

                <th>
                  Meter ID
                </th>

                <th>
                  Date
                </th>

                <th>
                  Anomaly Type
                </th>

                <th>
                  Anomaly Score
                </th>

                <th>
                  Risk Score
                </th>

                <th>
                  Priority
                </th>

                <th>
                  Validation
                </th>

                <th>
                  Action
                </th>

              </tr>

            </thead>


            <tbody>

              {paginatedAnomalies.length ===
              0 ? (

                <tr>

                  <td
                    colSpan="8"
                    className="empty-table"
                  >
                    No anomalies match
                    the selected
                    filters.
                  </td>

                </tr>

              ) : (

                paginatedAnomalies.map(
                  (item, index) => {

                    const anomalyScore =
                      Number(
                        item.anomaly_score ??
                        0
                      );


                    const riskScore =
                      Number(
                        item.risk_score ??
                        item.anomaly_score ??
                        0
                      );


                    const riskLevel =
                      String(
                        item.risk_level ||
                          getRiskLevel(
                            riskScore
                          )
                      ).toUpperCase();


                    const priorityLevel =
                      getPriorityLevel(
                        item
                      );


                    return (

                      <tr
                        key={
                          item.id ||
                          `${item.meter_id}-${item.detected_at}-${index}`
                        }
                      >


                        {/* METER */}

                        <td>

                          <strong>
                            {
                              item.meter_id
                            }
                          </strong>

                        </td>


                        {/* DATE */}

                        <td>

                          {formatDate(
                            item.detected_at ||
                              item.date
                          )}

                        </td>


                        {/* TYPE */}

                        <td>

                          <span className="anomaly-type">

                            {
                              formatAnomalyType(
                                item.anomaly_type
                              )
                            }

                          </span>

                        </td>


                        {/* ANOMALY SCORE */}

                        <td>

                          {anomalyScore.toFixed(
                            3
                          )}

                        </td>


                        {/* ANALYTICAL RISK */}

                        <td>

                          <strong>
                            {riskScore.toFixed(
                              1
                            )}
                          </strong>

                        </td>


                        {/* PRIORITY */}

                        <td>

                          <PriorityBadge
                            level={
                              priorityLevel
                            }
                          />

                        </td>


                        {/* VALIDATION */}

                        <td>

                          {item.ground_truth ===
                          true ? (

                            <span className="truth-yes">

                              <CheckCircle2
                                size={14}
                              />

                              Injected

                            </span>

                          ) : (

                            <span className="truth-no">
                              Analytical
                            </span>

                          )}

                        </td>


                        {/* ACTION */}

                        <td>

                          <Link
                            to={`/meters/${item.meter_id}`}
                            className="view-button"
                          >

                            <Eye
                              size={15}
                            />

                            Review

                          </Link>

                        </td>

                      </tr>

                    );
                  }
                )

              )}

            </tbody>

          </table>

        </div>


        {/* =================================================
            PAGINATION
        ================================================= */}

        <div className="pagination">

          <span>
            Page {safePage} of{" "}
            {totalPages}
          </span>


          <div>

            <button
              disabled={
                safePage === 1
              }
              onClick={() =>
                setCurrentPage(
                  Math.max(
                    1,
                    safePage - 1
                  )
                )
              }
              aria-label="Previous page"
            >

              <ChevronLeft
                size={17}
              />

            </button>


            <button
              disabled={
                safePage ===
                totalPages
              }
              onClick={() =>
                setCurrentPage(
                  Math.min(
                    totalPages,
                    safePage + 1
                  )
                )
              }
              aria-label="Next page"
            >

              <ChevronRight
                size={17}
              />

            </button>

          </div>

        </div>

      </div>


      {/* =================================================
          DISCLAIMER
      ================================================= */}

      <div className="investigation-notice">

        <ShieldAlert
          size={22}
        />

        <div>

          <strong>
            Analytical Flag — Not Proof
            of Electricity Theft
          </strong>

          <p>
            GridGuard identifies unusual
            consumption behavior using
            statistical and anomaly-detection
            signals. An anomaly or elevated
            investigation priority indicates
            that a meter deserves further
            review; it does not confirm
            electricity theft. Final decisions
            require additional operational
            evidence and field validation.
          </p>

        </div>

      </div>

    </div>
  );
}

export default Anomalies;
import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  Search,
  Eye,
  RefreshCw,
  Gauge,
  ChevronLeft,
  ChevronRight,
  ShieldAlert,
  Activity,
} from "lucide-react";

import {
  getMeters,
  getPriorities,
} from "../services/api";

import RiskBadge from "../components/RiskBadge";


/* =========================================================
   NORMALIZE PRIORITY RESPONSE
========================================================= */

function normalizePriorityRows(response) {
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

  if (Array.isArray(response?.priorities)) {
    return response.priorities;
  }

  return [];
}


/* =========================================================
   PRIORITY LEVEL
========================================================= */

function normalizePriorityLevel(priority) {
  const value = String(
    priority?.priority_level ||
    priority?.level ||
    ""
  ).toUpperCase();

  if (value === "VERY HIGH") {
    return "VERY HIGH";
  }

  if (value === "HIGH") {
    return "HIGH";
  }

  if (value === "MEDIUM") {
    return "MEDIUM";
  }

  if (value === "LOW") {
    return "LOW";
  }

  if (value === "NORMAL") {
    return "NORMAL";
  }

  return "—";
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
  if (!level || level === "—") {
    return (
      <span className="priority-badge priority-normal">
        Not Available
      </span>
    );
  }

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
   METER DATA MERGE
========================================================= */

function mergeMeterPriorityData(meters, priorities) {
  const priorityMap = new Map();

  priorities.forEach((priority) => {
    const meterId = String(
      priority?.meter_id || ""
    );

    if (meterId) {
      priorityMap.set(meterId, priority);
    }
  });

  return meters.map((meter) => {
    const meterId = String(
      meter?.meter_id || ""
    );

    const priority =
      priorityMap.get(meterId) || null;

    const priorityScore =
      priority?.investigation_priority_score ??
      priority?.priority_score ??
      null;

    const anomalyDays =
      priority?.anomaly_days ??
      0;

    const highRiskDays =
      priority?.high_risk_days ??
      0;

    const analyzedDays =
      priority?.analyzed_days ??
      0;

    const agreementRate =
      priority?.agreement_rate ??
      0;

    const anomalyRate =
      priority?.anomaly_rate ??
      0;

    return {
      ...meter,

      priority,

      priorityScore:
        priorityScore !== null
          ? Number(priorityScore)
          : null,

      priorityLevel:
        normalizePriorityLevel(priority),

      anomalyDays:
        Number(anomalyDays) || 0,

      highRiskDays:
        Number(highRiskDays) || 0,

      analyzedDays:
        Number(analyzedDays) || 0,

      agreementRate:
        Number(agreementRate) || 0,

      anomalyRate:
        Number(anomalyRate) || 0,
    };
  });
}


/* =========================================================
   METERS PAGE
========================================================= */

function Meters() {
  const [meters, setMeters] = useState([]);

  const [search, setSearch] = useState("");

  const [priorityFilter, setPriorityFilter] =
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

  const loadMeters = async () => {
    try {
      setLoading(true);
      setError("");

      const [
        metersResponse,
        prioritiesResponse,
      ] = await Promise.all([
        getMeters(),
        getPriorities(),
      ]);


      /* -----------------------------------------------
         METER DATA
      ----------------------------------------------- */

      const meterData =
        Array.isArray(metersResponse.data)
          ? metersResponse.data
          : metersResponse.data?.data || [];


      /* -----------------------------------------------
         PRIORITY DATA
      ----------------------------------------------- */

      const priorityData =
        normalizePriorityRows(
          prioritiesResponse
        );


      /* -----------------------------------------------
         MERGE
      ----------------------------------------------- */

      const mergedData =
        mergeMeterPriorityData(
          meterData,
          priorityData
        );


      setMeters(mergedData);

    } catch (err) {
      console.error(
        "Meter registry error:",
        err
      );

      setError(
        "Unable to load meter data."
      );

    } finally {
      setLoading(false);
    }
  };


  /* =======================================================
     INITIAL LOAD
  ======================================================= */

  useEffect(() => {
    loadMeters();
  }, []);


  /* =======================================================
     FILTER
  ======================================================= */

  const filteredMeters = useMemo(() => {
    const searchValue =
      search.toLowerCase().trim();

    return meters.filter((meter) => {

      const meterId =
        String(
          meter.meter_id || ""
        ).toLowerCase();

      const zone =
        String(
          meter.zone || ""
        ).toLowerCase();

      const priorityLevel =
        String(
          meter.priorityLevel || ""
        ).toUpperCase();


      /* -----------------------------------------------
         SEARCH
      ----------------------------------------------- */

      const matchesSearch =
        meterId.includes(searchValue) ||
        zone.includes(searchValue);


      /* -----------------------------------------------
         PRIORITY
      ----------------------------------------------- */

      const matchesPriority =
        priorityFilter === "ALL" ||
        priorityLevel === priorityFilter;


      return (
        matchesSearch &&
        matchesPriority
      );
    });

  }, [
    meters,
    search,
    priorityFilter,
  ]);


  /* =======================================================
     PAGINATION
  ======================================================= */

  const totalPages = Math.max(
    1,
    Math.ceil(
      filteredMeters.length /
        itemsPerPage
    )
  );


  const safePage = Math.min(
    currentPage,
    totalPages
  );


  const paginatedMeters =
    filteredMeters.slice(
      (safePage - 1) *
        itemsPerPage,

      safePage *
        itemsPerPage
    );


  /* =======================================================
     SEARCH CHANGE
  ======================================================= */

  const changeSearch = (value) => {
    setSearch(value);
    setCurrentPage(1);
  };


  /* =======================================================
     PRIORITY CHANGE
  ======================================================= */

  const changePriority = (value) => {
    setPriorityFilter(value);
    setCurrentPage(1);
  };


  /* =======================================================
     SUMMARY COUNTS
  ======================================================= */

  const totalMeters =
    meters.length;


  const veryHighPriority =
    meters.filter(
      (meter) =>
        meter.priorityLevel ===
        "VERY HIGH"
    ).length;


  const highPriority =
    meters.filter(
      (meter) =>
        meter.priorityLevel ===
        "HIGH"
    ).length;


  const mediumPriority =
    meters.filter(
      (meter) =>
        meter.priorityLevel ===
        "MEDIUM"
    ).length;


  const normalPriority =
    meters.filter(
      (meter) =>
        ["LOW", "NORMAL"].includes(
          meter.priorityLevel
        )
    ).length;


  /* =======================================================
     LOADING
  ======================================================= */

  if (loading) {
    return (
      <div className="loading-state">
        Loading meter data...
      </div>
    );
  }


  /* =======================================================
     PAGE
  ======================================================= */

  return (
    <div className="meters-page">


      {/* =================================================
          PAGE HEADER
      ================================================= */}

      <div className="page-heading">

        <div>
          <h2>
            Smart Meters
          </h2>

          <p>
            Monitor consumption behavior
            and prioritize meters for
            investigation.
          </p>
        </div>


        <button
          className="refresh-button"
          onClick={loadMeters}
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
          SUMMARY
      ================================================= */}

      <div className="meter-summary">


        {/* TOTAL */}

        <div className="summary-card">

          <div className="summary-icon">
            <Gauge size={20} />
          </div>

          <div>
            <span>
              Total Meters
            </span>

            <strong>
              {totalMeters}
            </strong>
          </div>

        </div>


        {/* VERY HIGH */}

        <div className="summary-card">

          <div className="summary-icon high">
            <ShieldAlert
              size={20}
            />
          </div>

          <div>
            <span>
              Very High Priority
            </span>

            <strong>
              {veryHighPriority}
            </strong>
          </div>

        </div>


        {/* HIGH */}

        <div className="summary-card">

          <div className="summary-icon high">
            <Activity
              size={20}
            />
          </div>

          <div>
            <span>
              High Priority
            </span>

            <strong>
              {highPriority}
            </strong>
          </div>

        </div>


        {/* MEDIUM */}

        <div className="summary-card">

          <div className="summary-icon normal">
            <Gauge size={20} />
          </div>

          <div>
            <span>
              Medium Priority
            </span>

            <strong>
              {mediumPriority}
            </strong>
          </div>

        </div>


      </div>


      {/* =================================================
          FILTER BAR
      ================================================= */}

      <div className="filter-panel">


        {/* SEARCH */}

        <div className="meter-search">

          <Search size={18} />

          <input
            value={search}
            onChange={(e) =>
              changeSearch(
                e.target.value
              )
            }
            placeholder="Search meter ID or zone..."
          />

        </div>


        {/* PRIORITY FILTER */}

        <select
          value={priorityFilter}
          onChange={(e) =>
            changePriority(
              e.target.value
            )
          }
          className="risk-filter"
        >

          <option value="ALL">
            All Priority Levels
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

      </div>


      {/* =================================================
          TABLE
      ================================================= */}

      <div className="table-panel">


        {/* TABLE HEADER */}

        <div className="table-header">

          <div>

            <h3>
              Meter Investigation Registry
            </h3>

            <p>
              Showing{" "}
              {paginatedMeters.length}{" "}
              of{" "}
              {filteredMeters.length}{" "}
              meters
            </p>

          </div>

        </div>


        {/* TABLE */}

        <div className="table-wrapper">

          <table>

            <thead>

              <tr>

                <th>
                  Meter ID
                </th>

                <th>
                  Zone
                </th>

                <th>
                  Consumer Type
                </th>

                <th>
                  Priority Score
                </th>

                <th>
                  Priority Level
                </th>

                <th>
                  Anomaly Days
                </th>

                <th>
                  Model Agreement
                </th>

                <th>
                  Action
                </th>

              </tr>

            </thead>


            <tbody>

              {paginatedMeters.length === 0 ? (

                <tr>

                  <td
                    colSpan="8"
                    className="empty-table"
                  >
                    No meters found.
                  </td>

                </tr>

              ) : (

                paginatedMeters.map(
                  (meter) => (

                    <tr
                      key={
                        meter.meter_id
                      }
                    >


                      {/* METER ID */}

                      <td>

                        <strong>
                          {
                            meter.meter_id
                          }
                        </strong>

                      </td>


                      {/* ZONE */}

                      <td>
                        {
                          meter.zone ||
                          "—"
                        }
                      </td>


                      {/* CONSUMER TYPE */}

                      <td>
                        {
                          meter.consumer_type ||
                          "Residential"
                        }
                      </td>


                      {/* PRIORITY SCORE */}

                      <td>

                        <strong>
                          {
                            meter.priorityScore !==
                            null
                              ? meter.priorityScore.toFixed(
                                  1
                                )
                              : "—"
                          }
                        </strong>

                      </td>


                      {/* PRIORITY LEVEL */}

                      <td>

                        <PriorityBadge
                          level={
                            meter.priorityLevel
                          }
                        />

                      </td>


                      {/* ANOMALY DAYS */}

                      <td>

                        <span className="table-metric">

                          {
                            meter.anomalyDays
                          }

                          {meter.analyzedDays >
                            0 && (
                            <small>
                              {" "}
                              /{" "}
                              {
                                meter.analyzedDays
                              }
                            </small>
                          )}

                        </span>

                      </td>


                      {/* MODEL AGREEMENT */}

                      <td>

                        {meter.priority ? (
                          <span className="agreement-value">

                            {
                              meter.agreementRate.toFixed(
                                1
                              )
                            }
                            %

                          </span>
                        ) : (
                          "—"
                        )}

                      </td>


                      {/* ACTION */}

                      <td>

                        <Link
                          to={`/meters/${meter.meter_id}`}
                          className="view-button"
                        >

                          <Eye size={16} />

                          View

                        </Link>

                      </td>


                    </tr>

                  )
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
          INTERPRETATION
      ================================================= */}

      <div className="priority-disclaimer">

        <ShieldAlert
          size={17}
        />

        <span>
          <strong>
            Investigation priority:
          </strong>{" "}
          The priority score represents
          the strength and persistence of
          abnormal consumption evidence.
          It does not indicate confirmed
          electricity theft or a probability
          of theft.
        </span>

      </div>

    </div>
  );
}

export default Meters;
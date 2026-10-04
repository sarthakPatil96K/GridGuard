import {
  Search,
  Bell,
  RefreshCw,
  UserCircle,
} from "lucide-react";

function Topbar() {
  return (
    <header className="topbar">

      <div className="topbar-left">
        <h1>Electricity Intelligence</h1>
        <p>
          Monitor consumption patterns and anomaly risks
        </p>
      </div>

      <div className="topbar-right">

        {/* Search */}
        <div className="search-box">
          <Search size={18} />

          <input
            type="text"
            placeholder="Search meter..."
          />
        </div>

        {/* Refresh */}
        <button
          className="topbar-icon-button"
          title="Refresh data"
          onClick={() => window.location.reload()}
        >
          <RefreshCw size={19} />
        </button>

        {/* Notifications */}
        <button
          className="icon-button"
          title="Notifications"
        >
          <Bell size={20} />

          <span className="notification-dot"></span>
        </button>

        {/* User Profile */}
        <div className="user-profile">

          <UserCircle size={34} />

          <div>
            <strong>Administrator</strong>
            <span>Utility Operations</span>
          </div>

        </div>

      </div>

    </header>
  );
}

export default Topbar;
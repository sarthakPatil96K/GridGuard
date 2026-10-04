import {
  LayoutDashboard,
  Gauge,
  TriangleAlert,
  BarChart3,
  ShieldCheck,
} from "lucide-react";

import { NavLink } from "react-router-dom";

function Sidebar() {
  const menuItems = [
    {
      name: "Dashboard",
      path: "/dashboard",
      icon: LayoutDashboard,
    },
    {
      name: "Meters",
      path: "/meters",
      icon: Gauge,
    },
    {
      name: "Anomalies",
      path: "/anomalies",
      icon: TriangleAlert,
    },
    {
      name: "Analytics",
      path: "/analytics",
      icon: BarChart3,
    },
  ];

  return (
    <aside className="sidebar">

      <div className="sidebar-logo">
        <div className="logo-icon">
          <ShieldCheck size={24} />
        </div>

        <div>
          <h2>GridGuard</h2>
          <span>Energy Intelligence</span>
        </div>
      </div>

      <nav className="sidebar-nav">

        <p className="nav-section-title">
          MONITORING
        </p>

        {menuItems.map((item) => {
          const Icon = item.icon;

          return (
            <NavLink
              key={item.path}
              to={item.path}
              className={({ isActive }) =>
                `nav-item ${isActive ? "active" : ""}`
              }
            >
              <Icon size={20} />
              <span>{item.name}</span>
            </NavLink>
          );
        })}

      </nav>

      <div className="sidebar-footer">
        <div className="system-status">
          <span className="status-dot"></span>

          <div>
            <strong>System Online</strong>
            <small>Analytics engine active</small>
          </div>
        </div>
      </div>

    </aside>
  );
}

export default Sidebar;
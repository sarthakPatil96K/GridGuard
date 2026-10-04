function KPICard({ title, value, subtitle, icon: Icon }) {
  return (
    <div className="kpi-card">
      <div className="kpi-icon">
        {Icon && <Icon size={22} />}
      </div>

      <div className="kpi-content">
        <p>{title}</p>
        <h2>{value}</h2>

        {subtitle && (
          <span className="kpi-subtitle">
            {subtitle}
          </span>
        )}
      </div>
    </div>
  );
}

export default KPICard;
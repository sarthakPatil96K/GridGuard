const RiskBadge = ({ level }) => {
  const styles = {
    "VERY HIGH": "bg-red-100 text-red-800 border-red-300",
    HIGH: "bg-orange-100 text-orange-800 border-orange-300",
    MEDIUM: "bg-yellow-100 text-yellow-800 border-yellow-300",
    LOW: "bg-blue-100 text-blue-800 border-blue-300",
    NORMAL: "bg-gray-100 text-gray-700 border-gray-300",
  };

  return (
    <span
      className={`inline-flex items-center rounded-full border px-3 py-1 text-xs font-semibold ${
        styles[level] || styles.NORMAL
      }`}
    >
      {level}
    </span>
  );
};

export default RiskBadge;
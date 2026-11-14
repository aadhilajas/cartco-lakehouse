export default function Header({ tab, loading, onRefresh }) {
  return (
    <header>
      <div className="crumb">
        WORKSPACE <span>/</span> {tab.toUpperCase()}
      </div>
      <div className="header-actions">
        <span className="fixture-pill">SYNTHETIC DATA</span>
        <button className="refresh" disabled={loading} onClick={onRefresh}>
          {loading ? "Refreshing…" : "↻ Refresh snapshot"}
        </button>
      </div>
    </header>
  );
}

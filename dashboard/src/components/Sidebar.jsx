import Icon from "./Icon.jsx";
export default function Sidebar({ tab, setTab, hosted }) {
  return (
    <aside>
      <a className="brand" href="#">
        <span className="brandmark">C</span>cartco
        <span className="branddot">.</span>
      </a>
      <div className="workspace">UNIFIED COMMERCE LAKEHOUSE</div>
      <nav aria-label="Views">
        {["business", "engineering", "lineage"].map((name) => (
          <button
            key={name}
            aria-current={tab === name ? "page" : undefined}
            onClick={() => setTab(name)}
          >
            <Icon kind={name} />
            {name[0].toUpperCase() + name.slice(1)}
          </button>
        ))}
      </nav>
      <div className="aside-bottom">
        <span className="small-dot" />
        {hosted ? "Published project demo" : "Local project preview"}
        <p>Synthetic data. Real pipeline.</p>
        {hosted && (
          <p>
            Saved results from the last published run. Processing runs on
            demand.
          </p>
        )}
        <a
          href="https://github.com/aadhilajas/cartco-lakehouse"
          target="_blank"
          rel="noreferrer"
        >
          Project repository ↗
        </a>
      </div>
    </aside>
  );
}

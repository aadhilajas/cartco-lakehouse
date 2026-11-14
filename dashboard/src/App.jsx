import { useState } from "react";
import Sidebar from "./components/Sidebar.jsx";
import Header from "./components/Header.jsx";
import Empty from "./components/Empty.jsx";
import BusinessView from "./views/BusinessView.jsx";
import EngineeringView from "./views/EngineeringView.jsx";
import LineageView from "./views/LineageView.jsx";
import { freshness } from "./lib/model.js";
import { hosted } from "./lib/config.js";
import { useSnapshot } from "./hooks/useSnapshot.js";
export default function App() {
  const { data, error, loading, refresh } = useSnapshot();
  const [tab, setTab] = useState("business"),
    [channel, setChannel] = useState("all"),
    [start, setStart] = useState(""),
    [end, setEnd] = useState("");
  return (
    <div className="shell">
      <Sidebar tab={tab} setTab={setTab} hosted={hosted} />
      <main>
        <Header tab={tab} loading={loading} onRefresh={refresh} />

        <section className="page">
          <div className="title-row">
            <div>
              <span className="eyebrow">ONE RELEASE. ONE SET OF NUMBERS.</span>
              <h1>
                {tab === "business"
                  ? "Commerce, reconciled."
                  : tab === "engineering"
                    ? "The evidence behind the numbers."
                    : "Follow every transformation."}
              </h1>
              <p className="intro">
                {tab === "business"
                  ? "A clear view of sales across your storefront, marketplace and stores."
                  : tab === "engineering"
                    ? "Quality gates and catalog snapshots from the same published release."
                    : "Declared column dependencies for the transformations that produced this release."}
              </p>
            </div>
          </div>
          {error && (
            <div role="alert" className="alert">
              {data ? "Refresh failed. Keeping the last valid snapshot. " : ""}
              {error}
              {!data &&
                (hosted
                  ? " No validated snapshot could be loaded from this deployment."
                  : " Start the local backend after exporting a validated release.")}
            </div>
          )}
          {!data ? (
            <Empty>
              {loading
                ? "Loading a validated release…"
                : "No published snapshot is available yet."}
            </Empty>
          ) : (
            <>
              <div className="release-strip">
                <span className="status-dot" />
                Validated release <code>{data.release_id.slice(0, 13)}</code>
                <span className="release-right">
                  {freshness(data)} · Exported{" "}
                  {new Date(data.exported_at).toLocaleString("en-IN")}
                </span>
              </div>
              {tab === "business" && (
                <BusinessView
                  data={data}
                  channel={channel}
                  setChannel={setChannel}
                  start={start}
                  setStart={setStart}
                  end={end}
                  setEnd={setEnd}
                />
              )}
              {tab === "engineering" && (
                <EngineeringView data={data} hosted={hosted} />
              )}
              {tab === "lineage" && <LineageView data={data} />}
              <footer>
                <span>CartCo / Aadhil A Kareem</span>
                <span>Synthetic commerce · immutable evidence · exact INR</span>
              </footer>
            </>
          )}
        </section>
      </main>
    </div>
  );
}

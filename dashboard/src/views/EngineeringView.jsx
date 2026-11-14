import Card from "../components/Card.jsx";
export default function EngineeringView({ data, hosted }) {
  return (
    <>
      <div className="metrics">
        <Card
          title="Quality gate"
          value={data.quality.success ? "Passed" : "Failed"}
          note={`${data.quality.suites.length} persisted Great Expectations suites`}
        />
        <Card
          title="Compute"
          value={"Spark " + data.compute.spark}
          note="Actual DataFrames, window deduplication and joins"
        />
        <Card
          title="Published tables"
          value={Object.keys(data.catalog).length}
          note="UUID and snapshot identities retained"
        />
      </div>
      <div className="two-col">
        <article className="panel">
          <h2>Output quality</h2>
          <p>Independent exact reconciliation precedes GE checks.</p>
          {data.quality.suites.map((suite) => (
            <div className="suite" key={suite.name}>
              <span>{suite.name.replaceAll("_", " ")}</span>
              <span className={suite.success ? "good" : "warning"}>
                {suite.success ? "PASS" : "FAIL"} · {suite.checks} checks
              </span>
            </div>
          ))}
        </article>
        <article className="panel">
          <h2>Run provenance</h2>
          <dl>
            <dt>Execution</dt>
            <dd>{data.execution.mode}</dd>
            <dt>Batch date</dt>
            <dd>{data.batch_date}</dd>
            <dt>Computed</dt>
            <dd>{new Date(data.computed_at).toLocaleString("en-IN")}</dd>
            <dt>Dashboard export</dt>
            <dd>{new Date(data.exported_at).toLocaleString("en-IN")}</dd>
            <dt>Pipeline duration</dt>
            <dd>
              {data.execution.duration_seconds === null
                ? "Not recorded"
                : data.execution.duration_seconds + " seconds"}
            </dd>
          </dl>
          <p className="footnote">
            Refresh reloads published results. It does not trigger a pipeline.{" "}
            {hosted && (
              <a
                href="https://github.com/aadhilajas/cartco-lakehouse/actions/workflows/publish-demo.yml"
                target="_blank"
                rel="noreferrer"
              >
                View pipeline runs ↗
              </a>
            )}
          </p>
        </article>
      </div>
      <article className="panel">
        <h2>Catalog registry</h2>
        <p>
          Identity of the data behind this release; snapshot numbers are kept as
          exact strings.
        </p>
        {Object.entries(data.catalog).map(([name, table]) => (
          <details key={name}>
            <summary>
              {name.replaceAll("_", " ")}
              <span>
                {table.snapshot_id ? "Snapshot retained" : "No snapshot"}
              </span>
            </summary>
            <p>{table.description}</p>
            <code className="block-code">
              {table.table}
              <br />
              UUID {table.uuid}
              <br />
              Snapshot {table.snapshot_id ?? "Unavailable"}
              <br />
              Owner {table.owner}
            </code>
          </details>
        ))}
      </article>
    </>
  );
}

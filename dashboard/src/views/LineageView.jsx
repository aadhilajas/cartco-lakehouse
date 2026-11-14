import React from "react";
export default function LineageView({ data }) {
  return (
    <>
      <div className="lineage-journey">
        {[
          "Four source protocols",
          "Bronze · raw evidence",
          "Silver · canonical state",
          "Gold · business marts",
        ].map((name, i) => (
          <React.Fragment key={name}>
            {i > 0 && <span className="arrow">→</span>}
            <div>
              <span>0{i + 1}</span>
              <strong>{name}</strong>
            </div>
          </React.Fragment>
        ))}
      </div>
      <article className="panel">
        <h2>Column-level derivations</h2>
        <p>
          These mappings describe implemented expressions.{" "}
          {data.lineage_transport.status === "verified"
            ? "Marquez backend verification is recorded."
            : "Marquez delivery has not been verified for this release."}
        </p>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Output</th>
                <th>Depends on</th>
                <th>Derivation</th>
              </tr>
            </thead>
            <tbody>
              {data.lineage.map((row) => (
                <tr key={row.output}>
                  <td>
                    <code>{row.output}</code>
                  </td>
                  <td>
                    {row.inputs.map((input) => (
                      <code className="dependency" key={input}>
                        {input}
                      </code>
                    ))}
                  </td>
                  <td>{row.description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </article>
    </>
  );
}

import Card from "../components/Card.jsx";
import Empty from "../components/Empty.jsx";
import { business, money, decimalMoney } from "../lib/model.js";
import { channelLabels as labels } from "../lib/config.js";
export default function BusinessView({
  data,
  channel,
  setChannel,
  start,
  setStart,
  end,
  setEnd,
}) {
  const result = business(data, channel, start, end);
  const dates = [...new Set(result.rows.map((r) => r.order_date))].sort();
  return (
    <>
      <div className="filters">
        <label>
          CHANNEL
          <select value={channel} onChange={(e) => setChannel(e.target.value)}>
            <option value="all">All channels</option>
            {Object.entries(labels).map(([key, label]) => (
              <option value={key} key={key}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <label>
          FROM
          <input
            type="date"
            value={start}
            onChange={(e) => setStart(e.target.value)}
          />
        </label>
        <label>
          TO
          <input
            type="date"
            value={end}
            min={start}
            onChange={(e) => setEnd(e.target.value)}
          />
        </label>
        <button
          className="clear"
          onClick={() => {
            setChannel("all");
            setStart("");
            setEnd("");
          }}
        >
          Reset filters
        </button>
        <span className="currency">INR · UTC +05:30 business dates</span>
      </div>
      <div className="metrics">
        <Card
          title="Completed sales"
          value={money(result.revenue)}
          note="Cancelled and refunded orders excluded"
        />
        <Card
          title="Completed orders"
          value={result.orders.toLocaleString("en-IN")}
          note="Latest source version; tombstones excluded"
        />
        <Card
          title="Average order value"
          value={result.average === null ? "—" : money(result.average)}
          note="Completed sales ÷ completed orders"
        />
      </div>
      <div className="two-col">
        <article className="panel">
          <div className="panel-heading">
            <div>
              <h2>Sales by business date</h2>
              <p>Exact completed-order revenue, across selected channels</p>
            </div>
            <span className="tag">INR</span>
          </div>
          {result.rows.length ? (
            <div className="chart">
              {dates.map((day) => {
                const rows = result.rows.filter((r) => r.order_date === day),
                  sum = business({ daily_revenue: rows }).revenue,
                  max = dates.reduce((m, d) => {
                    const n = business({
                      daily_revenue: result.rows.filter(
                        (r) => r.order_date === d,
                      ),
                    }).revenue;
                    return n > m ? n : m;
                  }, 0n);
                return (
                  <div className="chart-row" key={day}>
                    <span>{day}</span>
                    <div className="bar-track">
                      <div
                        className="bar"
                        style={{
                          width:
                            (max ? Number((sum * 10000n) / max) / 100 : 0) +
                            "%",
                        }}
                      />
                    </div>
                    <strong>{money(sum)}</strong>
                  </div>
                );
              })}
            </div>
          ) : (
            <Empty>No completed orders match these filters.</Empty>
          )}
        </article>
        <article className="panel channel-panel">
          <h2>Channel contribution</h2>
          <p>Completed sales in the selected date range</p>
          {Object.entries(labels)
            .filter(([key]) => channel === "all" || key === channel)
            .map(([key, label]) => {
              const value = business(data, key, start, end);
              return (
                <div className="channel-row" key={key}>
                  <span className={"channel-symbol " + key}>{label[0]}</span>
                  <div>
                    <strong>{label}</strong>
                    <small>{value.orders} completed orders</small>
                  </div>
                  <b>{money(value.revenue)}</b>
                </div>
              );
            })}
          <div className="footnote">
            Values reconcile to the published Gold marts.
          </div>
        </article>
      </div>
      <article className="panel">
        <div className="panel-heading">
          <div>
            <h2>Inventory turnover</h2>
            <p>
              Fixed standard cost, observed daily stock ·{" "}
              {data.inventory_period.period_start} to{" "}
              {data.inventory_period.period_end} (end exclusive)
            </p>
          </div>
          <span className="tag">ALL INVENTORY</span>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Source-qualified SKU</th>
                <th>Location</th>
                <th>COGS</th>
                <th>Average stock value</th>
                <th>Turnover</th>
                <th>Coverage</th>
              </tr>
            </thead>
            <tbody>
              {data.inventory.map((row) => (
                <tr key={row.sku + row.location_id}>
                  <td>{row.sku}</td>
                  <td>{row.location_id}</td>
                  <td>{money(row.cogs)}</td>
                  <td>
                    {row.average_daily_inventory_value === null
                      ? "Unavailable"
                      : decimalMoney(row.average_daily_inventory_value)}
                  </td>
                  <td>
                    {row.turnover === null
                      ? "Unavailable"
                      : Number(row.turnover).toLocaleString("en-IN", {
                          maximumFractionDigits: 4,
                        }) + "×"}
                  </td>
                  <td>
                    <span
                      className={
                        row.quality_status === "complete" ? "good" : "warning"
                      }
                    >
                      {row.observed_days}/{row.expected_days} days
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="footnote">
          Inventory scope is independent of the sales filters. Missing
          observations produce unavailable ratios, never assumed zero stock.
          Selling price is not inventory cost.
        </div>
      </article>
    </>
  );
}

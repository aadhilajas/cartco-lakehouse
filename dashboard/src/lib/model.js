export function cents(value) {
  if (typeof value !== 'string' || !/^\d+\.\d{2}$/.test(value)) throw new Error('Invalid exact INR amount');
  return BigInt(value.replace('.', ''));
}
export function money(value) {
  const amount = typeof value === 'bigint' ? value : cents(value);
  const whole = amount / 100n, remainder = String(amount % 100n).padStart(2,'0');
  return '₹' + whole.toLocaleString('en-IN') + '.' + remainder;
}
export function decimalMoney(value) {
  if (typeof value !== 'string' || !/^\d+\.\d+$/.test(value)) throw new Error('Invalid exact decimal');
  const [whole,fraction]=value.split('.'),digits=fraction.padEnd(3,'0');
  return money(BigInt(whole)*100n+BigInt(digits.slice(0,2))+(Number(digits[2])>=5?1n:0n));
}
export function validate(data) {
  if (data?.schema_version !== 1 || data?.is_synthetic !== true || !/^r[0-9a-f]{32}$/.test(data.release_id)) throw new Error('Unsupported snapshot');
  if (!Array.isArray(data.daily_revenue) || !Array.isArray(data.channels) || !Array.isArray(data.inventory)) throw new Error('Incomplete snapshot');
  for (const row of data.daily_revenue) {
    cents(row.total_revenue);
    if (!['shopify','amazon','pos'].includes(row.channel) || !/^\d{4}-\d{2}-\d{2}$/.test(row.order_date) || !Number.isSafeInteger(row.order_count) || row.order_count < 0) throw new Error('Invalid business row');
  }
  if (!data.quality?.success || !Array.isArray(data.quality.suites) || !data.catalog || !Array.isArray(data.lineage) || !data.compute || !data.execution || !data.inventory_period || !data.lineage_transport) throw new Error('Missing quality/catalog evidence');
  for(const row of data.inventory){cents(row.cogs);if(row.turnover!==null&&!/^\d+\.\d+$/.test(row.turnover))throw new Error('Invalid turnover');if(row.average_daily_inventory_value!==null)decimalMoney(row.average_daily_inventory_value);}
  return data;
}
export function business(data, channel='all', start='', end='') {
  const rows = data.daily_revenue.filter(row => (channel === 'all' || row.channel === channel) && (!start || row.order_date >= start) && (!end || row.order_date <= end));
  const revenue = rows.reduce((sum,row) => sum+cents(row.total_revenue),0n);
  const orders = rows.reduce((sum,row) => sum+row.order_count,0);
  const average = orders ? (revenue + BigInt(Math.floor(orders/2))) / BigInt(orders) : null;
  return {rows,revenue,orders,average};
}
export function freshness(data, now=Date.now()) {
  const age = now-Date.parse(data.exported_at);
  if (!Number.isFinite(age) || age < -300000) return 'Unknown freshness';
  return age > 86400000 ? 'Snapshot over 24 hours old' : 'Snapshot under 24 hours old';
}

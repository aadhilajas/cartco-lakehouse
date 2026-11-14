import test from 'node:test';
import assert from 'node:assert/strict';
import {cents,money,business,freshness,validate} from './model.js';
test('exact cents survive sums beyond floating point precision',()=>{assert.equal(cents('9999999999999999.99')+cents('0.01'),1000000000000000000n);assert.equal(money(0n),'₹0.00');assert.throws(()=>cents('1.001'));});
test('filters retain zero and empty average is unavailable',()=>{const d={daily_revenue:[{channel:'pos',order_date:'2026-11-12',order_count:2,total_revenue:'0.00'},{channel:'amazon',order_date:'2026-11-11',order_count:1,total_revenue:'10.50'}]};assert.equal(business(d,'pos').revenue,0n);assert.equal(business(d,'shopify').average,null);assert.equal(business(d,'all','2026-11-12').orders,2);});
test('stale and invalid snapshots are explicit',()=>{assert.equal(freshness({exported_at:'bad'}),'Unknown freshness');assert.equal(freshness({exported_at:'2026-11-10T00:00:00Z'},Date.parse('2026-11-12T00:00:00Z')),'Snapshot over 24 hours old');assert.throws(()=>validate({schema_version:99}));});

// Generates a sample orders CSV, streamed to disk.
//   node scripts/generate-orders.js [--rows 10000] [--invalid 0.02] [--customers 2000] [--out sample-data/orders.csv] [--seed 42]
import { createWriteStream } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { once } from 'node:events';
import { parseArgs } from 'node:util';

const { values: args } = parseArgs({
  options: {
    rows: { type: 'string', default: '10000' },
    invalid: { type: 'string', default: '0.02' },
    customers: { type: 'string', default: '2000' },
    out: { type: 'string', default: 'sample-data/orders.csv' },
    seed: { type: 'string', default: '42' },
  },
});

// Small deterministic PRNG (mulberry32) so the same seed gives the same file.
let seed = Number(args.seed) >>> 0;
function random() {
  seed = (seed + 0x6d2b79f5) >>> 0;
  let t = seed;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
const pick = (items) => items[Math.floor(random() * items.length)];

function uuid() {
  const hex = Array.from({ length: 32 }, () => Math.floor(random() * 16).toString(16));
  hex[12] = '4';
  hex[16] = ((parseInt(hex[16], 16) & 0x3) | 0x8).toString(16);
  const h = hex.join('');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

const STATUSES = ['pending', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded'];
const RANGE_START = Date.UTC(2024, 0, 1);
const RANGE_MS = Date.UTC(2025, 11, 31) - RANGE_START;

function validRow() {
  return {
    order_id: uuid(),
    customer_id: `CUST-${String(1 + Math.floor(random() * Number(args.customers))).padStart(6, '0')}`,
    order_date: new Date(RANGE_START + Math.floor(random() * RANGE_MS)).toISOString(),
    order_amount: (1 + random() * 2000).toFixed(2),
    status: pick(STATUSES),
  };
}

// Each corruption mimics a realistic data problem the ingestion must survive.
const CORRUPTIONS = [
  (r) => ({ ...r, order_id: '' }),
  (r) => ({ ...r, customer_id: '' }),
  (r) => ({ ...r, order_date: 'not-a-date' }),
  (r) => ({ ...r, order_date: '2024-02-30T10:00:00Z' }),
  (r) => ({ ...r, order_amount: 'abc' }),
  (r) => ({ ...r, order_amount: '-15.00' }),
  (r) => ({ ...r, order_amount: '12.345' }),
  (r) => ({ ...r, status: 'teleported' }),
  (r) => ({ ...r, __extraColumn: true }),
];

const csvField = (value) => (/[",\n\r]/.test(value) ? `"${value.replaceAll('"', '""')}"` : value);

await mkdir(path.dirname(path.resolve(args.out)), { recursive: true });
const out = createWriteStream(args.out);
const write = async (line) => {
  if (!out.write(`${line}\n`)) await once(out, 'drain');
};

// Header uses the spec's spelling "order_amout" on purpose; the parser accepts both spellings.
await write('order_id,customer_id,order_date,order_amout,status');
const total = Number(args.rows);
const invalidRatio = Number(args.invalid);
let invalid = 0;
for (let i = 0; i < total; i += 1) {
  let row = validRow();
  if (random() < invalidRatio) {
    row = pick(CORRUPTIONS)(row);
    invalid += 1;
  }
  const fields = [row.order_id, row.customer_id, row.order_date, row.order_amount, row.status].map(csvField);
  if (row.__extraColumn) fields.push('unexpected');
  await write(fields.join(','));
}
out.end();
await once(out, 'finish');
console.log(`wrote ${total} rows (${invalid} intentionally invalid) to ${args.out}`);

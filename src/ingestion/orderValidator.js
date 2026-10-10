import { PermanentJobError } from '../utils/errors.js';

export const REQUIRED_FIELDS = Object.freeze(['order_id', 'customer_id', 'order_date', 'order_amount', 'status']);

// Header spellings accepted for the canonical field names. `order_amout` is the spelling used in the spec.
const HEADER_ALIASES = Object.freeze({
  order_amout: 'order_amount',
  amount: 'order_amount',
  orderid: 'order_id',
  customerid: 'customer_id',
  orderdate: 'order_date',
  orderamount: 'order_amount',
  order_status: 'status',
});

export const ORDER_STATUSES = Object.freeze(
  new Set(['pending', 'processing', 'shipped', 'delivered', 'completed', 'cancelled', 'refunded', 'returned', 'failed']),
);
const STATUS_ALIASES = Object.freeze({ canceled: 'cancelled' });

const MAX_ID_LENGTH = 64;
// eslint-disable-next-line no-control-regex
const CONTROL_CHARS = /[\u0000-\u001f\u007f]/;
const AMOUNT_RE = /^\d{1,12}(\.\d{1,2})?$/;
// 2024-01-31 | 2024-01-31T10:20 | 2024-01-31 10:20:30.123 | ...+05:30 | ...Z
const DATE_RE = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2})(\.\d{1,6})?)?)?\s*(Z|[+-]\d{2}:?\d{2})?$/i;

export function normalizeHeaderName(name) {
  const normalized = String(name ?? '')
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, '_');
  return HEADER_ALIASES[normalized] ?? normalized;
}

/**
 * Maps the header row to column positions. Missing required columns make the whole file
 * unusable, so that is a permanent (non-retryable) job error. Extra columns are ignored.
 */
export function resolveHeader(headerRow) {
  const columns = headerRow.map(normalizeHeaderName);
  const index = {};
  columns.forEach((column, position) => {
    if (REQUIRED_FIELDS.includes(column)) {
      if (column in index) throw new PermanentJobError(`duplicate column "${column}" in header`);
      index[column] = position;
    }
  });
  const missing = REQUIRED_FIELDS.filter((field) => !(field in index));
  if (missing.length > 0) {
    throw new PermanentJobError(`header is missing required column(s): ${missing.join(', ')} (got: ${headerRow.join(', ')})`);
  }
  return {
    columnCount: columns.length,
    columns,
    /** Picks the required fields out of a raw record array. */
    pick(record) {
      const values = {};
      for (const field of REQUIRED_FIELDS) values[field] = record[index[field]] ?? '';
      return values;
    },
    /** Raw record keyed by (normalized) header name, for storing rejected rows readably. */
    toObject(record) {
      const object = {};
      record.forEach((value, position) => {
        object[columns[position] ?? `column_${position + 1}`] = value;
      });
      return object;
    },
  };
}

/**
 * Parses an ISO-8601-style timestamp. Values without an offset are interpreted as UTC so the
 * result never depends on the server's timezone. Rejects impossible dates such as 2024-02-30.
 * @returns {string|null} normalized ISO string (UTC) or null if invalid
 */
export function parseOrderDate(input) {
  const match = DATE_RE.exec(input);
  if (!match) return null;
  const [, year, month, day, hour = '00', minute = '00', second = '00', fraction = '', zone] = match;
  const y = Number(year);
  const mo = Number(month);
  const d = Number(day);
  if (y < 1900 || y > 2100 || mo < 1 || mo > 12 || Number(hour) > 23 || Number(minute) > 59 || Number(second) > 59) {
    return null;
  }
  const calendarCheck = new Date(Date.UTC(y, mo - 1, d));
  if (calendarCheck.getUTCMonth() !== mo - 1 || calendarCheck.getUTCDate() !== d) return null;

  let offset = 'Z';
  if (zone && zone.toUpperCase() !== 'Z') offset = zone.includes(':') ? zone : `${zone.slice(0, 3)}:${zone.slice(3)}`;
  const millis = Date.parse(`${year}-${month}-${day}T${hour}:${minute}:${second}${fraction.slice(0, 4)}${offset}`);
  return Number.isNaN(millis) ? null : new Date(millis).toISOString();
}

function validateId(value, field, errors) {
  if (!value) errors.push(`${field} is required`);
  else if (value.length > MAX_ID_LENGTH) errors.push(`${field} exceeds ${MAX_ID_LENGTH} characters`);
  else if (CONTROL_CHARS.test(value)) errors.push(`${field} contains control characters`);
}

/**
 * Validates and normalizes one row.
 * @param {Record<string,string>} values raw string values keyed by canonical field name
 * @returns {{ ok: true, order: object } | { ok: false, errors: string[] }}
 */
export function validateOrder(values) {
  const errors = [];
  const orderId = (values.order_id ?? '').trim();
  const customerId = (values.customer_id ?? '').trim();
  const rawDate = (values.order_date ?? '').trim();
  const rawAmount = (values.order_amount ?? '').trim();
  const rawStatus = (values.status ?? '').trim().toLowerCase();

  validateId(orderId, 'order_id', errors);
  validateId(customerId, 'customer_id', errors);

  let orderDate = null;
  if (!rawDate) errors.push('order_date is required');
  else if (!(orderDate = parseOrderDate(rawDate))) errors.push(`order_date "${rawDate}" is not a valid ISO-8601 date`);

  if (!rawAmount) errors.push('order_amount is required');
  else if (!AMOUNT_RE.test(rawAmount)) {
    errors.push(`order_amount "${rawAmount}" must be a non-negative decimal with at most 12 integer and 2 fraction digits`);
  }

  const status = STATUS_ALIASES[rawStatus] ?? rawStatus;
  if (!status) errors.push('status is required');
  else if (!ORDER_STATUSES.has(status)) errors.push(`status "${values.status}" is not one of: ${[...ORDER_STATUSES].join(', ')}`);

  if (errors.length > 0) return { ok: false, errors };
  return { ok: true, order: { orderId, customerId, orderDate, orderAmount: rawAmount, status } };
}

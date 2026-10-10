import { test } from 'node:test';
import assert from 'node:assert/strict';
import { normalizeHeaderName, parseOrderDate, resolveHeader, validateOrder } from '../src/ingestion/orderValidator.js';
import { PermanentJobError } from '../src/utils/errors.js';

const valid = {
  order_id: '7a0c1f6e-3b8d-4c61-9f2a-1b2c3d4e5f60',
  customer_id: 'CUST-000123',
  order_date: '2024-05-01T10:15:00Z',
  order_amount: '149.99',
  status: 'Shipped',
};

test('accepts and normalizes a valid row', () => {
  const result = validateOrder(valid);
  assert.equal(result.ok, true);
  assert.deepEqual(result.order, {
    orderId: valid.order_id,
    customerId: 'CUST-000123',
    orderDate: '2024-05-01T10:15:00.000Z',
    orderAmount: '149.99',
    status: 'shipped',
  });
});

test('collects every problem in a row', () => {
  const result = validateOrder({ order_id: '', customer_id: '', order_date: 'yesterday', order_amount: '1,000', status: 'lost' });
  assert.equal(result.ok, false);
  assert.equal(result.errors.length, 5);
});

for (const [field, value] of [
  ['order_amount', '-1.00'],
  ['order_amount', '10.999'],
  ['order_amount', '1e5'],
  ['order_amount', '1234567890123'],
  ['order_date', '2024-02-30'],
  ['order_date', '2024-13-01'],
  ['order_date', '2024-01-01T25:00:00Z'],
  ['order_date', '01/02/2024'],
  ['status', 'teleported'],
  ['order_id', 'x'.repeat(65)],
  ['customer_id', 'bad\u0000id'],
]) {
  test(`rejects ${field}=${JSON.stringify(value).slice(0, 30)}`, () => {
    assert.equal(validateOrder({ ...valid, [field]: value }).ok, false);
  });
}

test('maps "canceled" to "cancelled"', () => {
  assert.equal(validateOrder({ ...valid, status: 'canceled' }).order.status, 'cancelled');
});

test('dates without an offset are treated as UTC; offsets are honoured', () => {
  assert.equal(parseOrderDate('2024-03-10'), '2024-03-10T00:00:00.000Z');
  assert.equal(parseOrderDate('2024-03-10 08:30:00'), '2024-03-10T08:30:00.000Z');
  assert.equal(parseOrderDate('2024-03-10T08:30:00+05:30'), '2024-03-10T03:00:00.000Z');
  assert.equal(parseOrderDate('2024-03-10T08:30:00-0200'), '2024-03-10T10:30:00.000Z');
  assert.equal(parseOrderDate('2024-03-10T08:30:00.123456Z'), '2024-03-10T08:30:00.123Z');
  assert.equal(parseOrderDate('2024-02-29'), '2024-02-29T00:00:00.000Z');
  assert.equal(parseOrderDate('2023-02-29'), null);
});

test('header normalization accepts the spec spelling "order_amout" and casing/spacing variants', () => {
  assert.equal(normalizeHeaderName(' Order Amout '), 'order_amount');
  assert.equal(normalizeHeaderName('Customer-ID'), 'customer_id');
  const header = resolveHeader(['Order_ID', 'customer_id', 'order_date', 'order_amout', 'status', 'notes']);
  assert.equal(header.columnCount, 6);
  assert.deepEqual(header.pick(['o1', 'c1', '2024-01-01', '5', 'pending', 'x']), {
    order_id: 'o1',
    customer_id: 'c1',
    order_date: '2024-01-01',
    order_amount: '5',
    status: 'pending',
  });
});

test('a header missing required columns is a permanent error', () => {
  assert.throws(() => resolveHeader(['order_id', 'customer_id']), PermanentJobError);
  assert.throws(() => resolveHeader(['order_id', 'order_id', 'customer_id', 'order_date', 'order_amount', 'status']), PermanentJobError);
});

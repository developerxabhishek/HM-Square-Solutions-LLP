import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { LineSplitter, parseCsvLine } from '../src/ingestion/csvLines.js';

async function split(chunks, options) {
  const lines = [];
  await pipeline(Readable.from(chunks), new LineSplitter(options), async (source) => {
    for await (const line of source) lines.push(line);
  });
  return lines;
}

test('splits on LF and CRLF across arbitrary chunk boundaries and strips the BOM', async () => {
  const lines = await split(['﻿a,b\r', '\nc,', 'd\n', 'e,f']);
  assert.deepEqual(lines, [
    { lineNumber: 1, text: 'a,b' },
    { lineNumber: 2, text: 'c,d' },
    { lineNumber: 3, text: 'e,f' },
  ]);
});

test('decodes multi-byte UTF-8 characters split between chunks', async () => {
  const bytes = Buffer.from('id,name\n1,Zoë Ünal\n');
  const cut = bytes.indexOf(0xc3) + 1; // split inside the 2-byte "ë"
  const lines = await split([bytes.subarray(0, cut), bytes.subarray(cut)]);
  assert.equal(lines[1].text, '1,Zoë Ünal');
});

test('over-long lines are flagged and the following lines are unaffected', async () => {
  const lines = await split(['ok\n', 'x'.repeat(30), 'y'.repeat(30), '\nnext\n'], { maxLineLength: 50 });
  assert.deepEqual(lines, [
    { lineNumber: 1, text: 'ok' },
    { lineNumber: 2, tooLong: true },
    { lineNumber: 3, text: 'next' },
  ]);
});

test('parseCsvLine handles quoting rules', () => {
  assert.deepEqual(parseCsvLine('a, b ,c'), { fields: ['a', 'b', 'c'] });
  assert.deepEqual(parseCsvLine('"a,1","say ""hi""", "x" '), { fields: ['a,1', 'say "hi"', 'x'] });
  assert.deepEqual(parseCsvLine('a,,c,'), { fields: ['a', '', 'c', ''] });
  assert.deepEqual(parseCsvLine(''), { fields: [''] });
  assert.deepEqual(parseCsvLine('""'), { fields: [''] });
  assert.match(parseCsvLine('a,"unterminated').error, /unterminated/);
  assert.match(parseCsvLine('a,"12.00"x,b').error, /after closing quote/);
  assert.match(parseCsvLine('a,12"5,b').error, /unexpected quote/);
});

import { Transform } from 'node:stream';
import { StringDecoder } from 'node:string_decoder';

/**
 * Splits a byte stream into physical lines: { lineNumber, text } | { lineNumber, tooLong: true }.
 *
 * Design choice: one CSV record per physical line. Order fields (ids, dates, amounts, statuses)
 * never contain newlines, and line-scoped parsing means a malformed row (e.g. an unbalanced
 * quote) is rejected on its own instead of swallowing every following row, which is what a
 * multi-line-aware parser does when it never finds the closing quote.
 *
 * Runs as an object-mode Transform inside stream.pipeline, so backpressure is preserved.
 */
export class LineSplitter extends Transform {
  constructor({ maxLineLength = 64 * 1024 } = {}) {
    super({ readableObjectMode: true });
    this.decoder = new StringDecoder('utf8');
    this.maxLineLength = maxLineLength;
    this.pending = '';
    this.lineNumber = 0;
    this.discarding = false; // inside an over-long line, dropping text until its newline
  }

  _transform(chunk, _encoding, callback) {
    this.#consume(this.decoder.write(chunk));
    callback();
  }

  _flush(callback) {
    this.#consume(this.decoder.end());
    if (this.pending.length > 0 || this.discarding) this.#emit(this.pending);
    callback();
  }

  #consume(text) {
    let data = this.pending + text;
    this.pending = '';
    let start = 0;
    let newline;
    while ((newline = data.indexOf('\n', start)) !== -1) {
      this.#emit(data.slice(start, newline));
      start = newline + 1;
    }
    data = data.slice(start);
    if (this.discarding || data.length > this.maxLineLength) this.discarding = true;
    else this.pending = data;
  }

  #emit(line) {
    this.lineNumber += 1;
    if (this.discarding || line.length > this.maxLineLength) {
      this.discarding = false;
      this.push({ lineNumber: this.lineNumber, tooLong: true });
      return;
    }
    let text = line.endsWith('\r') ? line.slice(0, -1) : line;
    if (this.lineNumber === 1 && text.charCodeAt(0) === 0xfeff) text = text.slice(1); // UTF-8 BOM
    this.push({ lineNumber: this.lineNumber, text });
  }
}

const isBlank = (ch) => ch === ' ' || ch === '\t';

/**
 * Parses one RFC 4180 CSV line. Quoted fields may contain delimiters and "" escapes.
 * Unquoted fields are trimmed; a stray quote in an unquoted field is an error.
 * @returns {{ fields: string[] } | { error: string }}
 */
export function parseCsvLine(line, delimiter = ',') {
  const fields = [];
  const length = line.length;
  let i = 0;
  for (;;) {
    let j = i;
    while (j < length && isBlank(line[j])) j += 1;

    if (line[j] === '"') {
      let value = '';
      j += 1;
      for (;;) {
        const quote = line.indexOf('"', j);
        if (quote === -1) return { error: `unterminated quoted field starting at column ${i + 1}` };
        value += line.slice(j, quote);
        if (line[quote + 1] === '"') {
          value += '"';
          j = quote + 2;
        } else {
          j = quote + 1;
          break;
        }
      }
      while (j < length && isBlank(line[j])) j += 1;
      if (j < length && line[j] !== delimiter) return { error: `unexpected character after closing quote at column ${j + 1}` };
      fields.push(value);
    } else {
      const next = line.indexOf(delimiter, i);
      j = next === -1 ? length : next;
      const raw = line.slice(i, j);
      if (raw.includes('"')) return { error: `unexpected quote in unquoted field at column ${i + 1}` };
      fields.push(raw.trim());
    }

    if (j >= length) return { fields };
    i = j + 1; // skip the delimiter
    if (i === length) {
      fields.push(''); // trailing delimiter => empty last field
      return { fields };
    }
  }
}

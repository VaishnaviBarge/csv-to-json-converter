const fs = require('fs');
const readline = require('readline');
const { insertUsersBatch } = require('./db');

/**
 * Custom CSV line parser that follows RFC4180-ish rules:
 * - fields separated by commas
 * - fields can be double-quoted
 * - inside quoted fields, double quotes are escaped by double-double-quotes ("")
 */
function parseCSVLine(line) {
  const fields = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        // possible escaped quote
        if (i + 1 < line.length && line[i + 1] === '"') {
          cur += '"';
          i++; // skip next quote
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ',') {
        fields.push(cur);
        cur = '';
      } else {
        cur += ch;
      }
    }
  }
  // push last
  fields.push(cur);
  return fields;
}

/**
 * Set nested key in object by path array (creates intermediate objects).
 * Example: path = ['a','b','c'] will set obj.a.b.c = value
 */
function setNested(obj, path, value) {
  let cur = obj;
  for (let i = 0; i < path.length; i++) {
    const key = path[i];
    if (i === path.length - 1) {
      cur[key] = value;
    } else {
      if (cur[key] === undefined || typeof cur[key] !== 'object') cur[key] = {};
      cur = cur[key];
    }
  }
}

/**
 * Convert CSV headers array and a row array -> JSON object
 * Handles empty strings -> null where appropriate (you can adjust)
 */
function rowToObject(headers, row) {
  const obj = {};
  for (let i = 0; i < headers.length; i++) {
    const header = headers[i].trim();
    const valRaw = row[i] === undefined ? '' : row[i].trim();
    // treat empty string as null? Keep as '' or null depending on requirement. We'll keep empty string as null.
    const value = valRaw === '' ? null : valRaw;
    const path = header.split('.').map(p => p.trim());
    setNested(obj, path, value);
  }
  return obj;
}

/**
 * Build DB row from parsed JSON object
 * - name: firstName + ' ' + lastName (required)
 * - age: required, parsed to int
 * - address: all keys under top-level key 'address' (if any)
 * - additional_info: everything except name.* and age and address.*
 */
function buildDbRow(parsedObj) {
  // Extract firstName, lastName
  const firstName = parsedObj?.name?.firstName ?? null;
  const lastName = parsedObj?.name?.lastName ?? null;
  const ageRaw = parsedObj?.age ?? null;

  if (!firstName || !lastName) {
    throw new Error(`Missing mandatory name.firstName or name.lastName: ${JSON.stringify({ firstName, lastName })}`);
  }
  if (!ageRaw) {
    throw new Error(`Missing mandatory age for ${firstName} ${lastName}`);
  }

  const age = parseInt(ageRaw, 10);
  if (Number.isNaN(age)) throw new Error(`Invalid age value: ${ageRaw}`);

  const name = `${firstName} ${lastName}`;

  // address - take the entire parsedObj.address if present
  const address = parsedObj.address && Object.keys(parsedObj.address).length ? parsedObj.address : null;

  // additional_info - everything except name (and its subkeys) and age and address
  // Iterate recursively over parsedObj to pick remaining keys
  const additional_info = {};

  for (const topKey of Object.keys(parsedObj)) {
    if (topKey === 'name' || topKey === 'age' || topKey === 'address') continue;
    additional_info[topKey] = parsedObj[topKey];
  }

  const additional_info_final = Object.keys(additional_info).length ? additional_info : null;

  return { name, age, address, additional_info: additional_info_final };
}

/**
 * Main streaming CSV -> DB processor
 * - filePath: path to csv
 * - batchSize: number of rows per insert
 */
async function processCSV(filePath, batchSize = 1000, onProgress = ()=>{}) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(filePath)) {
      return reject(new Error(`CSV file not found at ${filePath}`));
    }

    const instream = fs.createReadStream(filePath);
    const rl = readline.createInterface({ input: instream, crlfDelay: Infinity });

    let headers = null;
    let bufferRows = [];
    let lineNo = 0;
    let processed = 0;

    rl.on('line', async (line) => {
      rl.pause(); 
      lineNo++;
      try {
        // Skip empty lines
        if (line.trim() === '') {
          rl.resume();
          return;
        }

        const fields = parseCSVLine(line);
        if (!headers) {
          // first non-empty line is headers
          headers = fields.map(h => h.trim());
          rl.resume();
          return;
        }

        const parsedObj = rowToObject(headers, fields);
        const dbRow = buildDbRow(parsedObj);
        bufferRows.push(dbRow);

        if (bufferRows.length >= batchSize) {
          await insertUsersBatch(bufferRows);
          processed += bufferRows.length;
          onProgress(processed);
          bufferRows = [];
        }
        rl.resume();
      } catch (err) {
        rl.close();
        return reject(new Error(`Error at line ${lineNo}: ${err.message}`));
      }
    });

    rl.on('close', async () => {
      try {
        if (bufferRows.length) {
          await insertUsersBatch(bufferRows);
          processed += bufferRows.length;
          onProgress(processed);
        }
        resolve({ processed, headers });
      } catch (err) {
        reject(err);
      }
    });

    rl.on('error', (err) => {
      reject(err);
    });
  });
}

module.exports = {
  parseCSVLine,
  processCSV
};

const { Pool } = require('pg');
const dotenv = require('dotenv');
dotenv.config();

const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    password:  process.env.DB_PASS,
    port: process.env.DB_PORT,
    database: process.env.DB_NAME
});

async function insertUsersBatch(rows) {
  // rows: array of { name, age, address (object|null), additional_info (object|null) }
  if (!rows.length) return;
  // Build parameterized query for multi-row insert
  const cols = ['"name"', '"age"', '"address"', '"additional_info"'];
  const params = [];
  const valuesSQL = rows.map((r, i) => {
    const baseIdx = i * cols.length;
    params.push(r.name);
    params.push(r.age);
    params.push(r.address ? JSON.stringify(r.address) : null);
    params.push(r.additional_info ? JSON.stringify(r.additional_info) : null);
    const placeholders = cols.map((_, j) => `$${baseIdx + j + 1}`);
    return `(${placeholders.join(',')})`;
  }).join(',');

  const sql = `INSERT INTO public.users (${cols.join(',')}) VALUES ${valuesSQL};`;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(sql, params);
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function computeAgeDistribution() {
  const client = await pool.connect();
  try {
    const res = await client.query('SELECT age, count(*) as cnt FROM public.users GROUP BY age;');
    const rows = res.rows;
    let total = 0;
    for (const r of rows) total += parseInt(r.cnt, 10);

    const groups = {
      '<20': 0,
      '20-40': 0, // 20 <= age <= 40
      '40-60': 0, // 41 <= age <= 60
      '>60': 0
    };

    for (const r of rows) {
      const age = parseInt(r.age, 10);
      const cnt = parseInt(r.cnt, 10);
      if (age < 20) groups['<20'] += cnt;
      else if (age >= 20 && age <= 40) groups['20-40'] += cnt;
      else if (age > 40 && age <= 60) groups['40-60'] += cnt;
      else groups['>60'] += cnt;
    }

    // Compute percentages 
    const percentages = {};
    for (const k of Object.keys(groups)) {
      percentages[k] = total === 0 ? 0 : Math.round((groups[k] / total) * 100);
    }

    return { total, groups, percentages };
  } finally {
    client.release();
  }
}

module.exports = {
  pool,
  insertUsersBatch,
  computeAgeDistribution
};

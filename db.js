require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.REDSHIFT_HOST,
  database: process.env.REDSHIFT_DB,
  user: process.env.REDSHIFT_USER,
  password: process.env.REDSHIFT_PASSWORD,
  port: parseInt(process.env.REDSHIFT_PORT) || 5439,
  ssl: { rejectUnauthorized: false },
  max: 5,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 15000,
});

pool.on('error', (err) => {
  console.error('Unexpected error on idle Redshift client', err);
});

module.exports = pool;

require('dotenv').config();
const express = require('express');
const cors = require('cors');

const cohortRoutes  = require('./routes/cohort');
const kpiRoutes     = require('./routes/kpi');
const filterRoutes  = require('./routes/filters');

const app = express();

app.use(cors());
app.use(express.json());

// Health check
app.get('/health', (req, res) => res.json({ status: 'ok', ts: new Date().toISOString() }));

// Connection test — exposes raw DB error for diagnosis
app.get('/debug/db', async (req, res) => {
  const pool = require('./db');
  try {
    const result = await pool.query('SELECT 1 AS ok');
    res.json({ connected: true, result: result.rows });
  } catch (err) {
    res.status(500).json({ connected: false, error: err.message, code: err.code });
  }
});

// Routes
app.use('/api/cohort',  cohortRoutes);
app.use('/api/kpi',     kpiRoutes);
app.use('/api/filters', filterRoutes);

// 404
app.use((req, res) => res.status(404).json({ error: 'Not found' }));

// Error handler
app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: 'Internal server error' });
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`Rider Retention API running on port ${PORT}`));

const express = require('express');
const pool = require('../db');
const router = express.Router();

// GET /api/filters — returns warehouses and static filter options
router.get('/', async (req, res) => {
  try {
    const warehouseResult = await pool.query(`
      SELECT warehouse_id, name
      FROM public_conformed.dim_warehouse
      WHERE is_active = true
        AND warehouse_id != '663dc1f47999894c9ebbd7d6'
      ORDER BY name
    `);

    res.json({
      warehouses: warehouseResult.rows,
      fleetLevels: [
        { value: '1', label: 'Level 1 — Bike' },
        { value: '2', label: 'Level 2 — Auto' },
        { value: '3', label: 'Level 3 — Zen (3-Wheeler)' },
      ],
      orderStatuses: [
        'delivered',
        'cancelled',
        'failed',
        'returned',
        'accepted',
        'dispatch',
        'ready',
        'rejected',
      ],
    });
  } catch (err) {
    console.error('Filters error:', err);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;

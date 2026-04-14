const express = require('express');
const pool = require('../db');
const router = express.Router();

const VALID_FLEET_LEVELS = {
  '1': "LOWER(r.vehicle_type) = 'bike'",
  '2': "LOWER(r.vehicle_type) = 'auto'",
  '3': "LOWER(r.vehicle_type) IN ('zen_small', 'zen_large')",
};
const VALID_STATUSES = new Set([
  'delivered','cancelled','failed','returned','accepted',
  'dispatch','ready','rejected','arrived',
]);

// GET /api/kpi
// Returns:
//   total_active_riders          — distinct riders with ≥1 trip since Monday
//   current_week_retention_pct   — % of prev week's riders who returned this week
//   prev_week_riders             — base count for tooltip context
router.get('/', async (req, res) => {
  try {
    const { fleet_level, warehouse_id, order_status } = req.query;
    const params = [];
    const extraJoins  = [];
    const extraWhere  = [];

    if (fleet_level && fleet_level !== 'all' && VALID_FLEET_LEVELS[fleet_level]) {
      extraWhere.push(VALID_FLEET_LEVELS[fleet_level]);
    }
    if (warehouse_id && warehouse_id !== 'all') {
      params.push(warehouse_id);
      extraWhere.push(`r.warehouse_id = $${params.length}`);
    }
    if (order_status && order_status !== 'all' && VALID_STATUSES.has(order_status)) {
      params.push(order_status);
      extraJoins.push(`JOIN public_mart_orders.fact_orders fo ON t.order_id = fo.order_id`);
      extraWhere.push(`fo.current_status = $${params.length}`);
    }

    const joinSql  = extraJoins.join('\n');
    const whereSql = extraWhere.length ? 'AND ' + extraWhere.join(' AND ') : '';

    const sql = `
      WITH curr_week AS (
        SELECT DISTINCT t.rider_id
        FROM public_mart_riders.fact_rider_trip_orders t
        JOIN public_conformed.dim_rider r ON t.rider_id = r.rider_id
        ${joinSql}
        WHERE TO_DATE(t.trip_date_id::varchar, 'YYYYMMDD') >= DATE_TRUNC('week', CURRENT_DATE)
          ${whereSql}
      ),
      prev_week AS (
        SELECT DISTINCT t.rider_id
        FROM public_mart_riders.fact_rider_trip_orders t
        JOIN public_conformed.dim_rider r ON t.rider_id = r.rider_id
        ${joinSql}
        WHERE TO_DATE(t.trip_date_id::varchar, 'YYYYMMDD') >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '7 days'
          AND TO_DATE(t.trip_date_id::varchar, 'YYYYMMDD')  < DATE_TRUNC('week', CURRENT_DATE)
          ${whereSql}
      ),
      retained AS (
        SELECT COUNT(DISTINCT c.rider_id) AS cnt
        FROM curr_week c
        WHERE EXISTS (SELECT 1 FROM prev_week p WHERE p.rider_id = c.rider_id)
      )
      SELECT
        (SELECT COUNT(*) FROM curr_week)                                               AS total_active_riders,
        (SELECT COUNT(*) FROM prev_week)                                               AS prev_week_riders,
        (SELECT cnt       FROM retained)                                               AS retained_riders,
        ROUND(
          (SELECT cnt FROM retained) * 100.0
          / NULLIF((SELECT COUNT(*) FROM prev_week), 0),
        1)                                                                             AS current_week_retention_pct
    `;

    const result = await pool.query(sql, params);
    res.json({ ...result.rows[0], updated_at: new Date().toISOString() });
  } catch (err) {
    console.error('KPI error:', err.message);
    res.status(500).json({ error: 'Failed to fetch KPI data' });
  }
});

module.exports = router;

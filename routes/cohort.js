const express = require('express');
const pool = require('../db');
const router = express.Router();

// Valid whitelists to prevent SQL injection on non-parameterised values
const VALID_FLEET_LEVELS = {
  '1': "LOWER(r.vehicle_type) = 'bike'",
  '2': "LOWER(r.vehicle_type) = 'auto'",
  '3': "LOWER(r.vehicle_type) IN ('zen_small', 'zen_large')",
};
const VALID_STATUSES = new Set([
  'delivered','cancelled','failed','returned','accepted',
  'dispatch','ready','rejected','arrived','placed',
  'fleet_pending','return_requested','return_accepted',
  'return_pickup','return_rejected','checkout_pending','abandoned',
]);

/**
 * Builds the parameterised retention SQL.
 * weeklyMode = true  → only last 3 weeks (Tab 2)
 * weeklyMode = false → all weeks from W1 2026 (Tab 1)
 *
 * No dependency on dim_date — week start computed directly
 * from fact_rider_trip_orders.trip_date_id (YYYYMMDD int)
 * using DATE_TRUNC('week', ...) which returns Monday in Redshift.
 */
function buildRetentionQuery(filters = {}, weeklyMode = false) {
  const { fleetLevel, warehouseId, orderStatus } = filters;
  const params = [];
  const whereClauses = [
    "TO_DATE(t.trip_date_id::varchar, 'YYYYMMDD') >= '2026-01-05'",
  ];
  let statusJoin = '';

  // Fleet level — whitelisted so safe to inline
  if (fleetLevel && fleetLevel !== 'all' && VALID_FLEET_LEVELS[fleetLevel]) {
    whereClauses.push(VALID_FLEET_LEVELS[fleetLevel]);
  }

  // Warehouse — parameterised
  if (warehouseId && warehouseId !== 'all') {
    params.push(warehouseId);
    whereClauses.push(`r.warehouse_id = $${params.length}`);
  }

  // Order status — whitelisted then parameterised
  if (orderStatus && orderStatus !== 'all' && VALID_STATUSES.has(orderStatus)) {
    params.push(orderStatus);
    statusJoin = `JOIN public_mart_orders.fact_orders fo ON t.order_id = fo.order_id`;
    whereClauses.push(`fo.current_status = $${params.length}`);
  }

  // Tab 2: restrict to last 3 weeks using date arithmetic (no dim_date needed)
  const weekFilter = weeklyMode
    ? `AND DATE_TRUNC('week', TO_DATE(t.trip_date_id::varchar, 'YYYYMMDD')) >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '14 days'`
    : '';

  const sql = `
    WITH rider_week_activity AS (
      SELECT DISTINCT
        t.rider_id,
        DATE_TRUNC('week', TO_DATE(t.trip_date_id::varchar, 'YYYYMMDD')) AS week_start
      FROM public_mart_riders.fact_rider_trip_orders t
      JOIN public_conformed.dim_rider r ON t.rider_id = r.rider_id
      ${statusJoin}
      WHERE ${whereClauses.join(' AND ')}
      ${weekFilter}
    ),
    cohort_base AS (
      SELECT week_start, COUNT(DISTINCT rider_id) AS active_riders
      FROM rider_week_activity
      GROUP BY week_start
    ),
    w1_retained AS (
      SELECT
        a.week_start,
        COUNT(DISTINCT b.rider_id) AS retained_w1
      FROM rider_week_activity a
      JOIN rider_week_activity b
        ON  a.rider_id  = b.rider_id
        AND b.week_start = DATEADD(week, 1, a.week_start)
      GROUP BY a.week_start
    ),
    w2_retained AS (
      SELECT
        a.week_start,
        COUNT(DISTINCT b.rider_id) AS retained_w2
      FROM rider_week_activity a
      JOIN rider_week_activity b
        ON  a.rider_id  = b.rider_id
        AND b.week_start = DATEADD(week, 2, a.week_start)
      GROUP BY a.week_start
    )
    SELECT
      TO_CHAR(c.week_start, 'YYYY-MM-DD')                                            AS week_start_date,
      DATEDIFF(week, '2026-01-05', c.week_start) + 1                                 AS user_week_number,
      c.active_riders,
      COALESCE(w1.retained_w1, 0)                                                    AS retained_w1,
      COALESCE(w2.retained_w2, 0)                                                    AS retained_w2,
      ROUND(COALESCE(w1.retained_w1, 0) * 100.0 / NULLIF(c.active_riders, 0), 1)    AS w1_pct,
      ROUND(COALESCE(w2.retained_w2, 0) * 100.0 / NULLIF(c.active_riders, 0), 1)    AS w2_pct
    FROM cohort_base c
    LEFT JOIN w1_retained w1 ON c.week_start = w1.week_start
    LEFT JOIN w2_retained w2 ON c.week_start = w2.week_start
    ORDER BY c.week_start
  `;

  return { sql, params };
}

// GET /api/cohort/monthly  — Tab 1: all weeks from W1 2026 to current
router.get('/monthly', async (req, res) => {
  try {
    const { fleet_level, warehouse_id, order_status } = req.query;
    const { sql, params } = buildRetentionQuery(
      { fleetLevel: fleet_level, warehouseId: warehouse_id, orderStatus: order_status },
      false
    );
    const result = await pool.query(sql, params);
    res.json({ data: result.rows, updated_at: new Date().toISOString() });
  } catch (err) {
    console.error('Monthly cohort error:', err.message);
    res.status(500).json({ error: 'Failed to fetch monthly cohort data' });
  }
});

// GET /api/cohort/weekly   — Tab 2: last 3 weeks, refreshed daily
router.get('/weekly', async (req, res) => {
  try {
    const { fleet_level, warehouse_id, order_status } = req.query;
    const { sql, params } = buildRetentionQuery(
      { fleetLevel: fleet_level, warehouseId: warehouse_id, orderStatus: order_status },
      true
    );
    const result = await pool.query(sql, params);
    res.json({ data: result.rows, updated_at: new Date().toISOString() });
  } catch (err) {
    console.error('Weekly cohort error:', err.message);
    res.status(500).json({ error: 'Failed to fetch weekly cohort data' });
  }
});

module.exports = router;

const express = require('express');
const pool = require('../db');
const router = express.Router();

// Valid whitelists to prevent SQL injection on non-parameterised values
const VALID_FLEET_LEVELS = { '1': "LOWER(r.vehicle_type) = 'bike'", '2': "LOWER(r.vehicle_type) = 'auto'", '3': "LOWER(r.vehicle_type) IN ('zen_small', 'zen_large')" };
const VALID_STATUSES = new Set(['delivered', 'cancelled', 'failed', 'returned', 'accepted', 'dispatch', 'ready', 'rejected', 'arrived', 'placed', 'fleet_pending', 'return_requested', 'return_accepted', 'return_pickup', 'return_rejected', 'checkout_pending', 'abandoned']);

/**
 * Builds the parameterised retention SQL.
 * weeklyMode = true  → only last 3 weeks (Tab 2)
 * weeklyMode = false → all weeks from W1 2026 (Tab 1)
 */
function buildRetentionQuery(filters = {}, weeklyMode = false) {
  const { fleetLevel, warehouseId, orderStatus } = filters;
  const params = [];
  const whereClauses = ["d.year = 2026", "d.week_of_year >= 2"];
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

  // Tab 2: restrict to last 3 weeks using actual trip data (dim_date has all 52 weeks)
  const weekFilter = weeklyMode
    ? `AND d.week_of_year >= (
          SELECT MAX(d2.week_of_year) - 2
          FROM public_mart_riders.fact_rider_trip_orders t2
          JOIN public_conformed.dim_date d2 ON t2.trip_date_id = d2.date_id
          WHERE d2.year = 2026
       )`
    : '';

  const sql = `
    WITH rider_week_activity AS (
      SELECT DISTINCT
        t.rider_id,
        d.week_of_year,
        d.year
      FROM public_mart_riders.fact_rider_trip_orders t
      JOIN public_conformed.dim_date d ON t.trip_date_id = d.date_id
      JOIN public_conformed.dim_rider r ON t.rider_id = r.rider_id
      ${statusJoin}
      WHERE ${whereClauses.join(' AND ')}
      ${weekFilter}
    ),
    week_starts AS (
      SELECT week_of_year, year, TO_CHAR(full_date, 'YYYY-MM-DD') AS week_start_date
      FROM public_conformed.dim_date
      WHERE year = 2026
        AND day_name = 'Monday'
    ),
    cohort_base AS (
      SELECT year, week_of_year, COUNT(DISTINCT rider_id) AS active_riders
      FROM rider_week_activity
      GROUP BY year, week_of_year
    ),
    w1_retained AS (
      SELECT
        a.year,
        a.week_of_year,
        COUNT(DISTINCT b.rider_id) AS retained_w1
      FROM rider_week_activity a
      JOIN rider_week_activity b
        ON  a.rider_id      = b.rider_id
        AND b.year          = a.year
        AND b.week_of_year  = a.week_of_year + 1
      GROUP BY a.year, a.week_of_year
    ),
    w2_retained AS (
      SELECT
        a.year,
        a.week_of_year,
        COUNT(DISTINCT b.rider_id) AS retained_w2
      FROM rider_week_activity a
      JOIN rider_week_activity b
        ON  a.rider_id      = b.rider_id
        AND b.year          = a.year
        AND b.week_of_year  = a.week_of_year + 2
      GROUP BY a.year, a.week_of_year
    )
    SELECT
      c.year,
      c.week_of_year,
      (c.week_of_year - 1)                                                         AS user_week_number,
      ws.week_start_date,
      c.active_riders,
      COALESCE(w1.retained_w1, 0)                                                  AS retained_w1,
      COALESCE(w2.retained_w2, 0)                                                  AS retained_w2,
      ROUND(COALESCE(w1.retained_w1, 0) * 100.0 / NULLIF(c.active_riders, 0), 1)  AS w1_pct,
      ROUND(COALESCE(w2.retained_w2, 0) * 100.0 / NULLIF(c.active_riders, 0), 1)  AS w2_pct
    FROM cohort_base c
    LEFT JOIN w1_retained w1 ON c.year = w1.year AND c.week_of_year = w1.week_of_year
    LEFT JOIN w2_retained w2 ON c.year = w2.year AND c.week_of_year = w2.week_of_year
    LEFT JOIN week_starts  ws ON c.year = ws.year AND c.week_of_year = ws.week_of_year
    ORDER BY c.year, c.week_of_year
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

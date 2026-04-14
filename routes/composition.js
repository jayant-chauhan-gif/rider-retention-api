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
  'dispatch','ready','rejected','arrived','placed',
  'fleet_pending','return_requested','return_accepted',
  'return_pickup','return_rejected','checkout_pending','abandoned',
]);

/**
 * GET /api/composition
 *
 * For each week from W1 2026, breaks down active riders by source:
 *   from_w1      — active last week too (retained)
 *   from_w2      — active 2 weeks ago, skipped last week (short reactivation)
 *   from_w3plus  — came back after 3+ weeks inactive
 *   new_riders   — first trip ever (under current fleet/warehouse filter)
 *
 * "New" is based on first-ever trip in the filtered segment, not just since 2026-01-05,
 * so riders active before Jan 5 won't incorrectly appear as new.
 */
router.get('/', async (req, res) => {
  try {
    const { fleet_level, warehouse_id, order_status } = req.query;
    const params = [];

    // Build filter clauses — reused across multiple CTEs
    const fleetClause =
      fleet_level && fleet_level !== 'all' && VALID_FLEET_LEVELS[fleet_level]
        ? VALID_FLEET_LEVELS[fleet_level]
        : null;

    let warehouseClause = null;
    if (warehouse_id && warehouse_id !== 'all') {
      params.push(warehouse_id);
      warehouseClause = `r.warehouse_id = $${params.length}`;
    }

    let statusJoin = '';
    let statusClause = null;
    if (order_status && order_status !== 'all' && VALID_STATUSES.has(order_status)) {
      params.push(order_status);
      statusJoin = `JOIN public_mart_orders.fact_orders fo ON t.order_id = fo.order_id`;
      statusClause = `fo.current_status = $${params.length}`;
    }

    // Dimension clauses (no date filter) — used to compute first-ever week per rider
    const dimClauses = ['1=1'];
    if (fleetClause)     dimClauses.push(fleetClause);
    if (warehouseClause) dimClauses.push(warehouseClause);

    // Activity clauses (with 2026 date filter)
    const actClauses = ["TO_DATE(t.trip_date_id::varchar, 'YYYYMMDD') >= '2026-01-05'"];
    if (fleetClause)     actClauses.push(fleetClause);
    if (warehouseClause) actClauses.push(warehouseClause);
    if (statusClause)    actClauses.push(statusClause);

    const sql = `
      WITH rider_first_week AS (
        -- Each rider's first-ever active week under the fleet/warehouse filter
        -- (no date cap so pre-2026 riders are not mis-classified as new)
        SELECT
          t.rider_id,
          DATE_TRUNC('week', TO_DATE(MIN(t.trip_date_id)::varchar, 'YYYYMMDD')) AS first_week
        FROM public_mart_riders.fact_rider_trip_orders t
        JOIN public_conformed.dim_rider r ON t.rider_id = r.rider_id
        WHERE ${dimClauses.join(' AND ')}
        GROUP BY t.rider_id
      ),
      rider_week_activity AS (
        SELECT DISTINCT
          t.rider_id,
          DATE_TRUNC('week', TO_DATE(t.trip_date_id::varchar, 'YYYYMMDD')) AS week_start
        FROM public_mart_riders.fact_rider_trip_orders t
        JOIN public_conformed.dim_rider r ON t.rider_id = r.rider_id
        ${statusJoin}
        WHERE ${actClauses.join(' AND ')}
      ),
      weekly_composition AS (
        SELECT
          a.week_start,
          a.rider_id,
          CASE
            WHEN rf.first_week = a.week_start   THEN 'new'
            WHEN w1.rider_id   IS NOT NULL       THEN 'from_w1'
            WHEN w2.rider_id   IS NOT NULL       THEN 'from_w2'
            ELSE                                      'from_w3plus'
          END AS src
        FROM rider_week_activity a
        JOIN rider_first_week rf
          ON a.rider_id = rf.rider_id
        LEFT JOIN rider_week_activity w1
          ON  a.rider_id   = w1.rider_id
          AND w1.week_start = DATEADD(week, -1, a.week_start)
        LEFT JOIN rider_week_activity w2
          ON  a.rider_id   = w2.rider_id
          AND w2.week_start = DATEADD(week, -2, a.week_start)
      )
      SELECT
        TO_CHAR(week_start, 'YYYY-MM-DD')                                                          AS week_start_date,
        DATEDIFF(week, '2026-01-05', week_start) + 1                                               AS user_week_number,
        COUNT(*)                                                                                    AS total_active,
        SUM(CASE WHEN src = 'from_w1'     THEN 1 ELSE 0 END)                                      AS from_w1,
        SUM(CASE WHEN src = 'from_w2'     THEN 1 ELSE 0 END)                                      AS from_w2,
        SUM(CASE WHEN src = 'from_w3plus' THEN 1 ELSE 0 END)                                      AS from_w3plus,
        SUM(CASE WHEN src = 'new'         THEN 1 ELSE 0 END)                                      AS new_riders,
        ROUND(SUM(CASE WHEN src = 'from_w1'     THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS from_w1_pct,
        ROUND(SUM(CASE WHEN src = 'from_w2'     THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS from_w2_pct,
        ROUND(SUM(CASE WHEN src = 'from_w3plus' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS from_w3plus_pct,
        ROUND(SUM(CASE WHEN src = 'new'         THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS new_pct
      FROM weekly_composition
      GROUP BY week_start
      ORDER BY week_start
    `;

    const result = await pool.query(sql, params);
    res.json({ data: result.rows, updated_at: new Date().toISOString() });
  } catch (err) {
    console.error('Composition error:', err.message);
    res.status(500).json({ error: 'Failed to fetch composition data' });
  }
});

module.exports = router;

INSERT INTO cdm.retention (
    cohort_week, week_0, week_1, week_2, week_3, week_4, week_5
)
WITH calendar_weeks AS (
    SELECT DISTINCT DATE_TRUNC('week', calendar_date)::date AS week_start
    FROM dim.calendar
    WHERE calendar_date >= '2025-03-31'
),
user_reg AS (
    SELECT
        id,
        DATE_TRUNC('week', registered_at)::date AS cohort_week
    FROM raw.users
),
cohort_sizes AS (
    SELECT
        cohort_week,
        COUNT(DISTINCT id) AS cohort_size
    FROM user_reg
    GROUP BY cohort_week
),
paid_enrollments AS (
    SELECT
        e.id,
        DATE_TRUNC('week', u.registered_at)::date AS cohort_week,
        DATE_TRUNC('week', e.enrolled_at)::date AS activity_week,
        ((DATE_TRUNC('week', e.enrolled_at)::date - DATE_TRUNC('week', u.registered_at)::date) / 7)::int AS week_number
    FROM raw.enrollments e
    JOIN raw.users u ON e.id = u.id
    WHERE e.payment_status = 'paid'
),
retention_counts AS (
    SELECT
        cohort_week,
        week_number,
        COUNT(DISTINCT id) AS retained_users
    FROM paid_enrollments
    WHERE week_number BETWEEN 0 AND 5
    GROUP BY cohort_week, week_number
),
weeks_grid AS (
    SELECT
        c.week_start AS cohort_week,
        c2.week_start AS activity_week,
        ((c2.week_start - c.week_start) / 7)::int AS week_number
    FROM calendar_weeks c
    CROSS JOIN calendar_weeks c2
    WHERE c2.week_start >= c.week_start AND c2.week_start <= c.week_start + INTERVAL '5 weeks'
),
retention_full AS (
    SELECT
        wg.cohort_week,
        wg.week_number,
        COALESCE(rc.retained_users, 0) AS retained_users
    FROM weeks_grid wg
    LEFT JOIN retention_counts rc
        ON rc.cohort_week = wg.cohort_week AND rc.week_number = wg.week_number
),
retention_with_sizes AS (
    SELECT
        rf.cohort_week,
        rf.week_number,
        ROUND(rf.retained_users::NUMERIC / COALESCE(cs.cohort_size, 1), 3) AS retention_rate
    FROM retention_full rf
    LEFT JOIN cohort_sizes cs ON rf.cohort_week = cs.cohort_week
)
SELECT
    cw.week_start AS cohort_week,
    MAX(CASE WHEN rw.week_number = 0 THEN rw.retention_rate END) AS week_0,
    MAX(CASE WHEN rw.week_number = 1 THEN rw.retention_rate END) AS week_1,
    MAX(CASE WHEN rw.week_number = 2 THEN rw.retention_rate END) AS week_2,
    MAX(CASE WHEN rw.week_number = 3 THEN rw.retention_rate END) AS week_3,
    MAX(CASE WHEN rw.week_number = 4 THEN rw.retention_rate END) AS week_4,
    MAX(CASE WHEN rw.week_number = 5 THEN rw.retention_rate END) AS week_5
FROM calendar_weeks cw
LEFT JOIN retention_with_sizes rw ON cw.week_start = rw.cohort_week
GROUP BY cw.week_start
ORDER BY cw.week_start;
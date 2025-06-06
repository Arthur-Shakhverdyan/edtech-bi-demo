INSERT INTO cdm.funnel (source, step_number, funnel_step, users_count, cv_from_prev, cv_from_reg)
WITH user_reg AS (SELECT u.id, s.source
                  FROM raw.users u
                           LEFT JOIN dim.source s ON u.source_id = s.id),
     free_trial_users AS (SELECT DISTINCT e.user_id, s.source
                          FROM raw.enrollments e
                                   JOIN raw.users u ON e.user_id = u.id
                                   LEFT JOIN dim.source s ON u.source_id = s.id
                          WHERE e.payment_status = 'free_trial'),
     paid_users AS (SELECT DISTINCT e.user_id, s.source
                    FROM raw.enrollments e
                             JOIN raw.users u ON e.user_id = u.id
                             LEFT JOIN dim.source s ON u.source_id = s.id
                    WHERE e.payment_status = 'paid'),
     free_to_paid_users AS (SELECT DISTINCT ft.user_id, ft.source
                            FROM free_trial_users ft
                                     JOIN paid_users p ON ft.user_id = p.user_id),
     counts_by_source AS (SELECT u.source,
                                 COUNT(DISTINCT u.id)        AS reg_count,
                                 COUNT(DISTINCT ft.user_id)  AS free_trial_count,
                                 COUNT(DISTINCT ftp.user_id) AS free_to_paid_count,
                                 COUNT(DISTINCT p.user_id)   AS paid_count
                          FROM user_reg u
                                   LEFT JOIN free_trial_users ft ON u.id = ft.user_id
                                   LEFT JOIN free_to_paid_users ftp ON u.id = ftp.user_id
                                   LEFT JOIN paid_users p ON u.id = p.user_id
                          GROUP BY u.source),
     counts_overall_normalized AS (SELECT 'общая'                              AS source,
                                          SUM(CASE WHEN step = 1 THEN cnt END) AS reg_count,
                                          SUM(CASE WHEN step = 2 THEN cnt END) AS free_trial_count,
                                          SUM(CASE WHEN step = 3 THEN cnt END) AS free_to_paid_count,
                                          SUM(CASE WHEN step = 4 THEN cnt END) AS paid_count
                                   FROM (SELECT 1 AS step, COUNT(DISTINCT id) AS cnt
                                         FROM raw.users
                                         UNION ALL
                                         SELECT 2 AS step, COUNT(DISTINCT user_id)
                                         FROM raw.enrollments
                                         WHERE payment_status = 'free_trial'
                                         UNION ALL
                                         SELECT 3 AS step, COUNT(DISTINCT ft.user_id)
                                         FROM (SELECT DISTINCT user_id
                                               FROM raw.enrollments
                                               WHERE payment_status = 'free_trial') ft
                                                  JOIN
                                              (SELECT DISTINCT user_id
                                               FROM raw.enrollments
                                               WHERE payment_status = 'paid') p ON ft.user_id = p.user_id
                                         UNION ALL
                                         SELECT 4 AS step, COUNT(DISTINCT user_id)
                                         FROM raw.enrollments
                                         WHERE payment_status = 'paid') AS sub
                                   GROUP BY source)
SELECT source,
       1              AS step_number,
       'registration' AS funnel_step,
       reg_count      AS users_count,
       NULL::NUMERIC  AS cv_from_prev,
       1.0            AS cv_from_reg
FROM counts_by_source

UNION ALL

SELECT source,
       2,
       'free_trial',
       free_trial_count,
       ROUND(free_trial_count::NUMERIC / NULLIF(reg_count, 0), 3),
       ROUND(free_trial_count::NUMERIC / NULLIF(reg_count, 0), 3)
FROM counts_by_source

UNION ALL

SELECT source,
       3,
       'free_trial_to_paid',
       free_to_paid_count,
       ROUND(free_to_paid_count::NUMERIC / NULLIF(free_trial_count, 0), 3),
       ROUND(free_to_paid_count::NUMERIC / NULLIF(reg_count, 0), 3)
FROM counts_by_source

UNION ALL

SELECT source,
       4,
       'paid',
       paid_count,
       ROUND(paid_count::NUMERIC / NULLIF(reg_count, 0), 3),
       ROUND(paid_count::NUMERIC / NULLIF(reg_count, 0), 3)
FROM counts_by_source

UNION ALL

SELECT source,
       1,
       'registration',
       reg_count,
       NULL::NUMERIC,
       1.0
FROM counts_overall_normalized

UNION ALL

SELECT source,
       2,
       'free_trial',
       free_trial_count,
       ROUND(free_trial_count::NUMERIC / NULLIF(reg_count, 0), 3),
       ROUND(free_trial_count::NUMERIC / NULLIF(reg_count, 0), 3)
FROM counts_overall_normalized

UNION ALL

SELECT source,
       3,
       'free_trial_to_paid',
       free_to_paid_count,
       ROUND(free_to_paid_count::NUMERIC / NULLIF(free_trial_count, 0), 3),
       ROUND(free_to_paid_count::NUMERIC / NULLIF(reg_count, 0), 3)
FROM counts_overall_normalized

UNION ALL

SELECT source,
       4,
       'paid',
       paid_count,
       ROUND(paid_count::NUMERIC / NULLIF(reg_count, 0), 3),
       ROUND(paid_count::NUMERIC / NULLIF(reg_count, 0), 3)
FROM counts_overall_normalized
ORDER BY source, step_number;
WITH t_1 AS (SELECT *
             FROM (SELECT imp_date, user_name, stay_hours, page FROM s2_stay_time_statis) AS t3
                      LEFT JOIN ((SELECT imp_date, user_name, page, 1 AS pv, user_name AS user_id
                                  FROM s2_pv_uv_statis) AS t4 LEFT JOIN s2_user_department AS s2_user_department0
                                 ON t4.user_name = s2_user_department0.user_name)
                                ON t3.user_name = s2_user_department0.user_name)
SELECT department
FROM t_1
WHERE (imp_date >= '2025-03-17' AND imp_date <= '2025-03-17')
GROUP BY department LIMIT 1000
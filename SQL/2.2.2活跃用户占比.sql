DROP TABLE IF EXISTS nc_2_1_1_active_users_ratio;
-- 1.1.1 计算当日活跃用户占比
CREATE TABLE IF NOT EXISTS nc_2_1_1_active_users_ratio (
    "DATE" DATE,
    "ACTIVE_USER_COUNT" BIGINT,
    "TOTAL_USER_COUNT" BIGINT,
    "ACTIVE_USER_RATIO" NUMERIC(5,4)
);

WITH total_users AS (
    SELECT COUNT(DISTINCT USERID) AS TOTAL_USERS 
    FROM nc_1_3_user_signal_count
)
INSERT INTO nc_2_1_1_active_users_ratio
SELECT 
    a."DATE",
    COUNT(DISTINCT a."USERID") AS "ACTIVE_USER_COUNT",
    tu.TOTAL_USERS AS "TOTAL_USER_COUNT",
    CAST(COUNT(DISTINCT a."USERID") AS NUMERIC) / NULLIF(tu.TOTAL_USERS, 0) AS "ACTIVE_USER_RATIO"
FROM 
    nc_2_1_active_users_1h a
CROSS JOIN 
    total_users tu
GROUP BY 
    a."DATE", tu.TOTAL_USERS
ORDER BY 
    a."DATE";
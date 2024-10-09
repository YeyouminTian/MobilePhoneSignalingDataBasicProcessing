--2.4按基站位置汇总判断出夜间当日最长停留位置的ID，每个位置的累计夜间停留用户总数（不重复计算用户）
DROP table IF EXISTS nc_2_1_4_user_night_longest_stay;
-- 创建表来存储用户每天晚上的最长停留位置
CREATE TABLE IF NOT EXISTS nc_2_1_4_user_night_longest_stay (
    "DATE" DATE,
    "USERID" VARCHAR(255),
    "LONGITUDE" FLOAT,
    "LATITUDE" FLOAT,
    "DURATION" INT
);

-- 插入每个用户每天晚上的最长停留位置
INSERT INTO nc_2_1_4_user_night_longest_stay
SELECT 
    "DATE",
    "USERID",
    "LONGITUDE",
    "LATITUDE",
    "DURATION"
FROM (
    SELECT 
        DATE("STARTTIME") AS "DATE",
        "USERID",
        "LONGITUDE",
        "LATITUDE",
        SUM("DURATION") AS "DURATION",
        ROW_NUMBER() OVER (PARTITION BY DATE("STARTTIME"), "USERID" ORDER BY SUM("DURATION") DESC) AS rn
    FROM 
        nc_user_stay_time_location
    WHERE 
        (EXTRACT(HOUR FROM "STARTTIME") >= 20 OR EXTRACT(HOUR FROM "STARTTIME") < 6)
    GROUP BY 
        DATE("STARTTIME"), "USERID", "LONGITUDE", "LATITUDE"
) subquery
WHERE rn = 1;

-- 创建表来存储每个位置的每日和七天内的累计夜间停留用户总数
CREATE TABLE IF NOT EXISTS nc_2_1_4_night_longest_stay_base_station_7days (
    "DATE" DATE,
    "LONGITUDE" FLOAT,
    "LATITUDE" FLOAT,
    "DAILY_USER_COUNT" BIGINT,
    "SEVEN_DAY_USER_COUNT" BIGINT
);

-- 插入每个位置的每日和七天内的累计夜间停留用户总数
INSERT INTO nc_2_1_4_night_longest_stay_base_station_7days
WITH daily_counts AS (
    SELECT 
        "DATE",
        "LONGITUDE",
        "LATITUDE",
        COUNT(DISTINCT "USERID") AS "DAILY_USER_COUNT"
    FROM 
        nc_2_1_4_user_night_longest_stay
    GROUP BY 
        "DATE", "LONGITUDE", "LATITUDE"
),
seven_day_counts AS (
    SELECT 
        end_date AS "DATE",
        "LONGITUDE",
        "LATITUDE",
        COUNT(DISTINCT "USERID") AS "SEVEN_DAY_USER_COUNT"
    FROM (
        SELECT 
            "LONGITUDE",
            "LATITUDE",
            "USERID",
            "DATE",
            "DATE" - INTERVAL '6 days' AS start_date,
            "DATE" AS end_date
        FROM 
            nc_2_1_4_user_night_longest_stay
    ) AS seven_day_window
    GROUP BY 
        end_date, "LONGITUDE", "LATITUDE"
)
SELECT 
    COALESCE(d."DATE", s."DATE") AS "DATE",
    COALESCE(d."LONGITUDE", s."LONGITUDE") AS "LONGITUDE",
    COALESCE(d."LATITUDE", s."LATITUDE") AS "LATITUDE",
    COALESCE(d."DAILY_USER_COUNT", 0) AS "DAILY_USER_COUNT",
    COALESCE(s."SEVEN_DAY_USER_COUNT", 0) AS "SEVEN_DAY_USER_COUNT"
FROM 
    daily_counts d
FULL OUTER JOIN 
    seven_day_counts s
ON 
    d."DATE" = s."DATE" AND d."LONGITUDE" = s."LONGITUDE" AND d."LATITUDE" = s."LATITUDE"
ORDER BY 
    "DATE"


--计算每个基站七天合并的总数
--计算汇总数据
DROP table IF EXISTS nc_2_1_4_night_longest_stay_base_station_user_count;

CREATE TABLE IF NOT EXISTS nc_2_1_4_night_longest_stay_base_station_user_count (
    "LONGITUDE" FLOAT,
    "LATITUDE" FLOAT,
    "TOTAL_USER_COUNT" BIGINT
);

-- 插入每个位置的每日和七天内的累计夜间停留用户总数
INSERT INTO nc_2_1_4_night_longest_stay_base_station_user_count

SELECT
    "LONGITUDE",
    "LATITUDE",
    sum("DAILY_USER_COUNT") AS "TOTAL_USER_COUNT"
FROM
    nc_2_1_4_night_longest_stay_base_station_7days
GROUP BY
    "LONGITUDE", "LATITUDE";
DROP table IF EXISTS nc_3_1_2_user_longest_stay_base_station;
CREATE TABLE IF NOT EXISTS nc_3_1_2_user_longest_stay_base_station (
    "USERID" VARCHAR(255),
    "LONGITUDE" FLOAT8,
    "LATITUDE" FLOAT8,
    "OCCURRENCE_COUNT" INT,
    "TOTAL_DAYS" INT,
    "PERCENTAGE" FLOAT8
);

-- 插入数据
WITH user_total_days AS (
    SELECT "USERID", COUNT(DISTINCT "DATE") AS total_days
    FROM nc_2_1_3_1_60min_user_night_longest_stay
    GROUP BY "USERID"
),
user_stays AS (
    SELECT 
        "USERID",
        "LONGITUDE",
        "LATITUDE",
        COUNT(DISTINCT "DATE") AS occurrence_count
    FROM 
        nc_2_1_3_1_60min_user_night_longest_stay
    GROUP BY 
        "USERID", "LONGITUDE", "LATITUDE"
),
user_max_stays AS (
    SELECT 
        us.*,
        utd.total_days,
        CAST(us.occurrence_count AS FLOAT) / NULLIF(utd.total_days, 0) AS percentage,
        ROW_NUMBER() OVER (PARTITION BY us."USERID" ORDER BY us.occurrence_count DESC) AS rn
    FROM 
        user_stays us
    JOIN
        user_total_days utd ON us."USERID" = utd."USERID"
)
INSERT INTO nc_3_1_2_user_longest_stay_base_station
SELECT 
    "USERID",
    "LONGITUDE",
    "LATITUDE",
    occurrence_count AS "OCCURRENCE_COUNT",
    total_days AS "TOTAL_DAYS",
    percentage AS "PERCENTAGE"
FROM 
    user_max_stays
WHERE 
    rn = 1 AND percentage > 0.5;

-- 新增查询：统计不同经纬度下的人数
DROP TABLE IF EXISTS nc_3_1_2_location_user_count;
CREATE TABLE IF NOT EXISTS nc_3_1_2_location_user_count (
    "LONGITUDE" FLOAT8,
    "LATITUDE" FLOAT8,
    "USER_COUNT" INT
);

INSERT INTO nc_3_1_2_location_user_count
SELECT 
    "LONGITUDE",
    "LATITUDE",
    COUNT(DISTINCT "USERID") AS "USER_COUNT"
FROM 
    nc_3_1_2_user_longest_stay_base_station
GROUP BY 
    "LONGITUDE", "LATITUDE"
ORDER BY 
    "USER_COUNT" DESC;

-- 可以添加一个查询来显示结果
-- SELECT * FROM nc_3_1_2_location_user_count ORDER BY "USER_COUNT" DESC LIMIT 10;
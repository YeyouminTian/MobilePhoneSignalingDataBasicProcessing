DROP TABLE IF EXISTS tianyeyoumin.nc_2_1_3_1_30min_user_night_longest_stay;
DROP TABLE IF EXISTS tianyeyoumin.nc_2_1_3_1_60min_user_night_longest_stay;
DROP TABLE IF EXISTS tianyeyoumin.nc_2_1_3_1_90min_user_night_longest_stay;

-- 创建30分钟停留表
CREATE TABLE IF NOT EXISTS tianyeyoumin.nc_2_1_3_1_30min_user_night_longest_stay (
    "DATE" DATE,
    "USERID" VARCHAR(255),
    "LONGITUDE" FLOAT,
    "LATITUDE" FLOAT,
    "DURATION" INT
);

-- 创建60分钟停留表
CREATE TABLE IF NOT EXISTS tianyeyoumin.nc_2_1_3_1_60min_user_night_longest_stay (
    "DATE" DATE,
    "USERID" VARCHAR(255),
    "LONGITUDE" FLOAT,
    "LATITUDE" FLOAT,
    "DURATION" INT
);

-- 创建90分钟停留表
CREATE TABLE IF NOT EXISTS tianyeyoumin.nc_2_1_3_1_90min_user_night_longest_stay (
    "DATE" DATE,
    "USERID" VARCHAR(255),
    "LONGITUDE" FLOAT,
    "LATITUDE" FLOAT,
    "DURATION" INT
);

-- 创建aggregated_stays表
CREATE TABLE IF NOT EXISTS tianyeyoumin.nc_2_1_3_1_aggregated_stays (
    "DATE" DATE,
    "USERID" VARCHAR(255),
    "LONGITUDE" FLOAT,
    "LATITUDE" FLOAT,
    "DURATION" INT,
    rn INT
);

-- 创建一个临时表来存储所有的用户停留记录
CREATE TEMPORARY TABLE temp_all_stays AS
SELECT 
    "USERID",
    "LONGITUDE",
    "LATITUDE",
    "STARTTIME",
    "STARTTIME" + INTERVAL '1 minute' * "DURATION_MIN" AS "ENDTIME",
    "DURATION_MIN"
FROM 
    nc_user_stay_time_location
ORDER BY 
    "USERID", "STARTTIME";

-- 修改 overlaps_night 函数
CREATE OR REPLACE FUNCTION overlaps_night(start_time TIMESTAMP, end_time TIMESTAMP)
RETURNS INTERVAL AS $$
DECLARE
    night_start TIME := '20:00:00'::TIME;
    night_end TIME := '06:00:00'::TIME;
    overlap INTERVAL;
    day_start TIMESTAMP;
    next_day_start TIMESTAMP;
BEGIN
    day_start := DATE_TRUNC('day', start_time);
    next_day_start := day_start + INTERVAL '1 day';

    IF start_time::TIME >= night_start THEN
        -- 开始于当天夜间
        overlap := LEAST(end_time, next_day_start + night_end) - start_time;
    ELSIF end_time::TIME <= night_end THEN
        -- 结束于次日凌晨
        overlap := end_time - GREATEST(start_time, day_start + night_start);
    ELSIF start_time::TIME < night_start AND end_time::TIME > night_end THEN
        -- 跨越整个夜间
        overlap := INTERVAL '10 hours';
    ELSIF start_time::TIME < night_start THEN
        -- 开始于夜间之前，结束于夜间
        overlap := end_time - (day_start + night_start);
    ELSE
        -- 开始于凌晨，结束于夜间之后
        overlap := (day_start + night_end) - start_time;
    END IF;
    RETURN overlap;
END;
$$ LANGUAGE plpgsql;

-- 修改 night_stays CTE
WITH night_stays AS (
    SELECT 
        "USERID",
        "LONGITUDE",
        "LATITUDE",
        "STARTTIME",
        "ENDTIME",
        overlaps_night("STARTTIME", "ENDTIME") AS night_duration
    FROM 
        temp_all_stays
    WHERE 
        overlaps_night("STARTTIME", "ENDTIME") > INTERVAL '0'
),
continuous_night_stays AS (
    SELECT 
        ns.*,
        CASE 
            WHEN ns."STARTTIME" > LAG(ns."ENDTIME", 1) OVER (PARTITION BY ns."USERID", ns."LONGITUDE", ns."LATITUDE" ORDER BY ns."STARTTIME") + INTERVAL '10 hours'
            THEN 1 
            ELSE 0 
        END AS new_stay
    FROM 
        night_stays ns
),
stay_groups AS (
    SELECT 
        *,
        SUM(new_stay) OVER (PARTITION BY "USERID", "LONGITUDE", "LATITUDE" ORDER BY "STARTTIME") AS stay_group
    FROM 
        continuous_night_stays
)
INSERT INTO tianyeyoumin.nc_2_1_3_1_aggregated_stays
SELECT 
    DATE("STARTTIME") AS "DATE",
    "USERID",
    "LONGITUDE",
    "LATITUDE",
    EXTRACT(EPOCH FROM SUM(night_duration)) / 60 AS "DURATION",
    ROW_NUMBER() OVER (PARTITION BY "USERID", DATE("STARTTIME") ORDER BY SUM(night_duration) DESC) AS rn
FROM 
    stay_groups
GROUP BY 
    DATE("STARTTIME"), "USERID", "LONGITUDE", "LATITUDE", stay_group;

-- 从aggregated_stays表中插入数据到其他表
INSERT INTO tianyeyoumin.nc_2_1_3_1_30min_user_night_longest_stay
SELECT "DATE", "USERID", "LONGITUDE", "LATITUDE", "DURATION"
FROM tianyeyoumin.nc_2_1_3_1_aggregated_stays
WHERE "DURATION" > 1800 AND rn = 1;

INSERT INTO tianyeyoumin.nc_2_1_3_1_60min_user_night_longest_stay
("DATE", "USERID", "LONGITUDE", "LATITUDE", "DURATION")
SELECT "DATE", "USERID", "LONGITUDE", "LATITUDE", "DURATION"
FROM tianyeyoumin.nc_2_1_3_1_aggregated_stays
WHERE "DURATION" > 3600 AND rn = 1;

INSERT INTO tianyeyoumin.nc_2_1_3_1_90min_user_night_longest_stay
("DATE", "USERID", "LONGITUDE", "LATITUDE", "DURATION")
SELECT "DATE", "USERID", "LONGITUDE", "LATITUDE", "DURATION"
FROM tianyeyoumin.nc_2_1_3_1_aggregated_stays
WHERE "DURATION" > 5400 AND rn = 1;
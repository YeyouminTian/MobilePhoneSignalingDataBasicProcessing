DROP TABLE IF EXISTS tianyeyoumin.nc_2_1_3_1_30min_user_night_longest_stay;
DROP TABLE IF EXISTS tianyeyoumin.nc_2_1_3_1_60min_user_night_longest_stay;
DROP TABLE IF EXISTS tianyeyoumin.nc_2_1_3_1_90min_user_night_longest_stay;
DROP TABLE IF EXISTS tianyeyoumin.nc_2_1_3_1_aggregated_stays;

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

-- 填充aggregated_stays表
WITH night_stays AS (
    SELECT 
        "DATE",
        "USERID",
        "LONGITUDE",
        "LATITUDE",
        SUM("DURATION_MIN") AS "DURATION",
        MIN("STARTTIME") AS "MIN_STARTTIME"
    FROM 
        nc_user_stay_time_location
    WHERE 
        (EXTRACT(HOUR FROM "STARTTIME") >= 20 OR EXTRACT(HOUR FROM "STARTTIME") < 6)
    GROUP BY 
        "DATE", "USERID", "LONGITUDE", "LATITUDE"
),
continuous_night_stays AS (
    SELECT 
        ns.*,
        CASE 
            WHEN ns."MIN_STARTTIME" > LAG(ns."MIN_STARTTIME", 1) OVER (PARTITION BY ns."USERID", ns."LONGITUDE", ns."LATITUDE" ORDER BY ns."DATE", ns."MIN_STARTTIME") + INTERVAL '10 hours'
            THEN 1 
            ELSE 0 
        END AS new_stay
    FROM 
        night_stays ns
),
stay_groups AS (
    SELECT 
        *,
        SUM(new_stay) OVER (PARTITION BY "USERID", "LONGITUDE", "LATITUDE" ORDER BY "DATE", "MIN_STARTTIME") AS stay_group
    FROM 
        continuous_night_stays
)
INSERT INTO tianyeyoumin.nc_2_1_3_1_aggregated_stays
SELECT 
    "DATE",
    "USERID",
    "LONGITUDE",
    "LATITUDE",
    SUM("DURATION") AS "DURATION",
    ROW_NUMBER() OVER (PARTITION BY "USERID", "DATE" ORDER BY SUM("DURATION") DESC) AS rn
FROM 
    stay_groups
GROUP BY 
    "DATE", "USERID", "LONGITUDE", "LATITUDE", stay_group;

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


DROP TABLE IF EXISTS tianyeyoumin.nc_daily_users_with_residence;

-- 创建新表来存储每日有居住地的用户统计
CREATE TABLE IF NOT EXISTS tianyeyoumin.nc_daily_users_with_residence (
    "DATE" DATE PRIMARY KEY,
    "USER_COUNT" INT
);

-- 从60min_user_night_longest_stay表插入数据到新表
INSERT INTO tianyeyoumin.nc_daily_users_with_residence ("DATE", "USER_COUNT")
SELECT 
    "DATE",
    COUNT(DISTINCT "USERID") AS "USER_COUNT"
FROM tianyeyoumin.nc_2_1_3_1_60min_user_night_longest_stay
GROUP BY "DATE"
ORDER BY "DATE";

-- 查看新表的内容
SELECT * FROM tianyeyoumin.nc_daily_users_with_residence
ORDER BY "DATE";
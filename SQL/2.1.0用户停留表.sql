DROP TABLE IF EXISTS nc_user_stay_time_location;

-- 创建用户停留时间表
CREATE TABLE IF NOT EXISTS nc_user_stay_time_location (
    "DATE" DATE,
    "USERID" VARCHAR(255),
    "LONGITUDE" FLOAT,
    "LATITUDE" FLOAT,
    "STARTTIME" TIMESTAMP,
    "ENDTIME" TIMESTAMP,
    "DURATION" FLOAT,
    "DURATION_MIN" FLOAT  
);

WITH user_location_stays AS (
    SELECT 
        "USERID",
        "LONGITUDE",
        "LATITUDE",
        "STARTTIME"::TIMESTAMP AS "STARTTIME",
        DATE("STARTTIME"::TIMESTAMP) AS "DATE"
    FROM 
        nanchang_alldata
),
location_changes AS (
    SELECT 
        *,
        CASE 
            WHEN LAG("LONGITUDE") OVER (PARTITION BY "USERID" ORDER BY "STARTTIME") != "LONGITUDE" 
                 OR LAG("LATITUDE") OVER (PARTITION BY "USERID" ORDER BY "STARTTIME") != "LATITUDE"
                 OR LAG("LONGITUDE") OVER (PARTITION BY "USERID" ORDER BY "STARTTIME") IS NULL
            THEN 1
            ELSE 0
        END AS location_change,
        LEAD("STARTTIME") OVER (PARTITION BY "USERID" ORDER BY "STARTTIME") AS next_starttime
    FROM user_location_stays
),
stay_groups AS (
    SELECT 
        *,
        SUM(location_change) OVER (PARTITION BY "USERID" ORDER BY "STARTTIME") AS stay_group
    FROM location_changes
),
grouped_stays AS (
    SELECT 
        "USERID",
        "LONGITUDE",
        "LATITUDE",
        "DATE",
        MIN("STARTTIME") AS "STARTTIME",
        MAX(COALESCE(next_starttime, "STARTTIME")) AS "ENDTIME",
        EXTRACT(EPOCH FROM (MAX(COALESCE(next_starttime, "STARTTIME")) - MIN("STARTTIME"))) AS "DURATION",
        EXTRACT(EPOCH FROM (MAX(COALESCE(next_starttime, "STARTTIME")) - MIN("STARTTIME"))) / 60 AS "DURATION_MIN"
    FROM stay_groups
    GROUP BY 
        "USERID", "LONGITUDE", "LATITUDE", "DATE", stay_group
)
INSERT INTO nc_user_stay_time_location
SELECT 
    "DATE",
    "USERID",
    "LONGITUDE",
    "LATITUDE",
    "STARTTIME",
    "ENDTIME",
    "DURATION",
    "DURATION_MIN"
FROM 
    grouped_stays
WHERE 
    "ENDTIME" IS NOT NULL
ORDER BY 
    "USERID", "STARTTIME";
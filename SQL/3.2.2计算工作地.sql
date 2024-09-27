DROP table IF EXISTS nc_3_2_2_user_work_location;
-- 3.2.2 计算用户的工作地，工作日当天9:00-16:00，停留时间超过1小时，计算累计最大停留时间位置，出现天数大于等于4天的当日工作地相同，则该位置为用户的工作地。
-- 创建新表存储用户工作地
CREATE TABLE IF NOT EXISTS nc_3_2_2_user_work_location (
    "USERID" VARCHAR(255),
    "LONGITUDE" FLOAT,
    "LATITUDE" FLOAT,
    "APPEARANCE_COUNT" INT,
    "TOTAL_WORKDAYS" INT,
    "APPEARANCE_RATIO" FLOAT
);

-- 插入数据
WITH user_workdays AS (
    SELECT 
        "USERID",
        COUNT(DISTINCT "DATE") AS total_workdays
    FROM 
        nc_3_2_1_user_workday_longest_stay
    GROUP BY 
        "USERID"
),
location_counts AS (
    SELECT 
        "USERID",
        "LONGITUDE",
        "LATITUDE",
        COUNT(DISTINCT "DATE") AS appearance_count
    FROM 
        nc_3_2_1_user_workday_longest_stay
    GROUP BY 
        "USERID", "LONGITUDE", "LATITUDE"
)
INSERT INTO nc_3_2_2_user_work_location
SELECT 
    lc."USERID",
    lc."LONGITUDE",
    lc."LATITUDE",
    lc.appearance_count,
    uw.total_workdays,
    CAST(lc.appearance_count AS FLOAT) / uw.total_workdays AS appearance_ratio
FROM 
    location_counts lc
JOIN 
    user_workdays uw ON lc."USERID" = uw."USERID"
WHERE 
    CAST(lc.appearance_count AS FLOAT) / uw.total_workdays > 0.5;
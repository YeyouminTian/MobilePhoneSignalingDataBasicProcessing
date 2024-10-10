--3.1.2 推算每个用户日间当日最长停留基站位置（ 9:00-17:00 时间内，累计停留时间超过60分钟）活跃用户的夜最长位置表
DROP TABLE IF EXISTS nc_2_1_3_1_60min_user_day_longest_stay;

CREATE TABLE nc_2_1_3_1_60min_user_day_longest_stay (
    "DATE" DATE,
    "USERID" VARCHAR(255),
    "LONGITUDE" FLOAT,
    "LATITUDE" FLOAT,
    "DURATION" INT
);

INSERT INTO nc_2_1_3_1_60min_user_day_longest_stay
WITH user_stay_time AS (
    SELECT 
        "DATE",
        "USERID",
        "LONGITUDE",
        "LATITUDE",
        SUM("DURATION") AS "DURATION",
        ROW_NUMBER() OVER (PARTITION BY "DATE", "USERID" ORDER BY SUM("DURATION") DESC) AS rn
    FROM 
        nc_user_stay_time_location
    WHERE 
        EXTRACT(HOUR FROM "STARTTIME") >= 9 
        AND EXTRACT(HOUR FROM "STARTTIME") < 17
    GROUP BY 
        "DATE", "USERID", "LONGITUDE", "LATITUDE"
    HAVING 
        SUM("DURATION") > 60
)
SELECT 
    "DATE",
    "USERID",
    "LONGITUDE",
    "LATITUDE",
    "DURATION"
FROM 
    user_stay_time
WHERE 
    rn = 1;


-- 插入数据：计算每个经纬度的不同用户数量
INSERT INTO nc_2_3_2_day_longest_stay_location_user_count
SELECT 
    "DATE",
    "LONGITUDE",
    "LATITUDE",
    COUNT(DISTINCT "USERID") as "USER_COUNT"
FROM 
    nc_2_1_3_1_60min_user_day_longest_stay
GROUP BY 
    "DATE", "LONGITUDE", "LATITUDE";

-- 可以添加一个索引来优化查询性能（可选）
CREATE INDEX idx_location_user_count ON nc_2_3_2_day_longest_stay_location_user_count ("DATE", "LONGITUDE", "LATITUDE");
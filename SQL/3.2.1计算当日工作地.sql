DROP table IF EXISTS nc_3_2_1_user_workday_longest_stay;
-- 3.2.1 计算当日工作地，工作日当天9:00-16:00，停留时间超过1小时，计算累计最大停留时间位置，则该网格为当日工作地。
-- 创建新表来存储工作日的日间最长停留位置
CREATE TABLE IF NOT EXISTS nc_3_2_1_user_workday_longest_stay (
    "DATE" DATE,
    "USERID" VARCHAR(255),
    "LONGITUDE" FLOAT,
    "LATITUDE" FLOAT,
    "DURATION" INT
);

-- 插入数据
INSERT INTO nc_3_2_1_user_workday_longest_stay
SELECT 
    a."DATE",
    a."USERID",
    a."LONGITUDE",
    a."LATITUDE",
    a."DURATION"
FROM 
    nc_2_1_3_1_60min_user_day_longest_stay a
WHERE 
    EXTRACT(DOW FROM a."DATE") BETWEEN 1 AND 5;  -- 只选择工作日（星期一到星期五）

-- 创建新表来存储每个经纬度（基站）的用户数量
DROP TABLE IF EXISTS nc_3_2_1_location_user_count;
CREATE TABLE IF NOT EXISTS nc_3_2_1_location_user_count (
    "DATE" DATE,
    "LONGITUDE" FLOAT,
    "LATITUDE" FLOAT,
    "USER_COUNT" INT
);

-- 插入数据：计算每个经纬度的不同用户数量
INSERT INTO nc_3_2_1_location_user_count
SELECT 
    "DATE",
    "LONGITUDE",
    "LATITUDE",
    COUNT(DISTINCT "USERID") as "USER_COUNT"
FROM 
    nc_3_2_1_user_workday_longest_stay
GROUP BY 
    "DATE", "LONGITUDE", "LATITUDE";

-- 可以添加一个索引来优化查询性能（可选）
CREATE INDEX idx_location_user_count ON nc_3_2_1_location_user_count ("DATE", "LONGITUDE", "LATITUDE");

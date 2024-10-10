-- 创建新表来存储每个地址的工作人数和居住人数
DROP TABLE IF EXISTS nc_3_4_address_work_home_count;
CREATE TABLE IF NOT EXISTS nc_3_4_address_work_home_count (
    "LONGITUDE" FLOAT,
    "LATITUDE" FLOAT,
    "WORK_COUNT" INT DEFAULT 0,
    "HOME_COUNT" INT DEFAULT 0
);

-- 插入数据：统计每个地址的工作人数和居住人数
WITH work_counts AS (
    SELECT 
        "LONGITUDE",
        "LATITUDE",
        COUNT(DISTINCT "USERID") AS "WORK_COUNT"
    FROM 
        nc_3_2_2_user_work_location
    GROUP BY 
        "LONGITUDE", "LATITUDE"
),
home_counts AS (
    SELECT 
        "LONGITUDE",
        "LATITUDE",
        COUNT(DISTINCT "USERID") AS "HOME_COUNT"
    FROM 
        nc_3_1_2_user_longest_stay_base_station
    GROUP BY 
        "LONGITUDE", "LATITUDE"
)
INSERT INTO nc_3_4_address_work_home_count
SELECT 
    COALESCE(w."LONGITUDE", h."LONGITUDE") AS "LONGITUDE",
    COALESCE(w."LATITUDE", h."LATITUDE") AS "LATITUDE",
    COALESCE(w."WORK_COUNT", 0) AS "WORK_COUNT",
    COALESCE(h."HOME_COUNT", 0) AS "HOME_COUNT"
FROM 
    work_counts w
FULL OUTER JOIN 
    home_counts h
ON 
    w."LONGITUDE" = h."LONGITUDE" AND w."LATITUDE" = h."LATITUDE";

-- 查询结果
SELECT * FROM nc_3_4_address_work_home_count
ORDER BY ("WORK_COUNT" + "HOME_COUNT") DESC
LIMIT 10;
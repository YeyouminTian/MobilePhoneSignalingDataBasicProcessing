-- 创建新表来存储用户的工作地和居住地
DROP TABLE IF EXISTS nc_3_3_user_work_home_location;
CREATE TABLE IF NOT EXISTS nc_3_3_user_work_home_location (
    "USERID" VARCHAR(255),
    "WORK_LONGITUDE" FLOAT,
    "WORK_LATITUDE" FLOAT,
    "HOME_LONGITUDE" FLOAT,
    "HOME_LATITUDE" FLOAT
);

-- 插入数据：匹配用户的工作地和居住地，只包含两者都存在的记录
INSERT INTO nc_3_3_user_work_home_location
SELECT 
    w."USERID",
    w."LONGITUDE" AS "WORK_LONGITUDE",
    w."LATITUDE" AS "WORK_LATITUDE",
    h."LONGITUDE" AS "HOME_LONGITUDE",
    h."LATITUDE" AS "HOME_LATITUDE"
FROM 
    nc_3_2_2_user_work_location w
INNER JOIN 
    nc_3_1_2_user_longest_stay_base_station h
ON 
    w."USERID" = h."USERID"
WHERE 
    w."LONGITUDE" IS NOT NULL
    AND w."LATITUDE" IS NOT NULL
    AND h."LONGITUDE" IS NOT NULL
    AND h."LATITUDE" IS NOT NULL;

-- 查询结果
SELECT * FROM nc_3_3_user_work_home_location LIMIT 10;
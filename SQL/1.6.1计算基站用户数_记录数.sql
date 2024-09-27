--### 6.1 计算一日内每个基站上到达过的用户总数（不重复计算用户）
DROP TABLE IF EXISTS tianyeyoumin.nc_1_6_1_base_station_user_count;

CREATE TABLE IF NOT EXISTS tianyeyoumin.nc_1_6_1_base_station_user_count (
    "DATE" DATE,
    "CGI" BIGINT,
    "USER_COUNT" BIGINT,
    "SIGNAL_COUNT" BIGINT
);

INSERT INTO tianyeyoumin.nc_1_6_1_base_station_user_count
SELECT 
    DATE_TRUNC('day', "STARTTIME"::TIMESTAMP)::DATE AS "DATE",  -- 提取日期部分
    "CGI" AS "CGI",
    COUNT(DISTINCT "USERID") AS "USER_COUNT",          -- 统计每个基站上到达过的用户总数
    COUNT(*) AS "SIGNAL_COUNT"                  -- 统计每个基站上到达过的记录总数
FROM 
    tianyeyoumin.nanchang_alldata
GROUP BY 
    DATE_TRUNC('day', "STARTTIME"::TIMESTAMP),               -- 按天分组
    "CGI"
ORDER BY 
    "DATE";

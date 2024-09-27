CREATE TABLE IF NOT EXISTS tianyeyoumin.nc_1_5_hourly_signal_count (
    "DATE" DATE,          -- 日期部分
    "HOUR" SMALLINT,      -- 小时部分
    SIGNAL_COUNT BIGINT     -- 记录计数
);
INSERT INTO tianyeyoumin.nc_1_5_hourly_signal_count

SELECT 
    DATE_TRUNC('day', "STARTTIME"::TIMESTAMP)::DATE AS "DATE",  -- 提取日期部分
    EXTRACT(HOUR FROM "STARTTIME"::TIMESTAMP) AS "HOUR",       -- 提取小时部分
    COUNT(*) AS SIGNAL_COUNT          -- 统计每个小时内记录的数量

FROM 
    tianyeyoumin.nanchang_alldata
GROUP BY 
    DATE_TRUNC('day', "STARTTIME"::TIMESTAMP),                -- 按天分组
    EXTRACT(HOUR FROM "STARTTIME"::TIMESTAMP)                   -- 按小时分组
ORDER BY 
    "DATE", "HOUR";
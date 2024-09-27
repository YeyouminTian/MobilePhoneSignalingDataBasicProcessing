--## 4、计算24小时用户数的时变。计算"nanchang_alldata"表中24小时用户数的每小时的变化，分别按照日期输出表格，（1小时一档，0:00- 0:59'59，均算0点）
DROP TABLE IF EXISTS "nc_1_4_hourly_user_count";

CREATE TABLE IF NOT EXISTS "nc_1_4_hourly_user_count" (
    "DATE" DATE,          -- 日期部分
    "HOUR" SMALLINT,      -- 小时部分
    USER_COUNT BIGINT     -- 用户计数
);

INSERT INTO "nc_1_4_hourly_user_count"

SELECT 
    DATE_TRUNC('day', "STARTTIME"::TIMESTAMP)::DATE AS "DATE",  -- 提取日期部分
    EXTRACT(HOUR FROM "STARTTIME"::TIMESTAMP) AS "HOUR",       -- 提取小时部分
    COUNT(DISTINCT "USERID") AS USER_COUNT          -- 统计每个小时内不同用户的数量

FROM 
    nanchang_alldata
GROUP BY 
    DATE_TRUNC('day', "STARTTIME"::TIMESTAMP),                 -- 按天分组
    EXTRACT(HOUR FROM "STARTTIME"::TIMESTAMP)                  -- 按小时分组
ORDER BY 
    "DATE", "HOUR";
DROP TABLE IF EXISTS nc_2_1_active_users_1h;

--## 1.1求出当日活跃用户（在南昌境内累计出现1小时，排除过路者），生成一个活跃用户表。当日活跃用户占比
CREATE TABLE IF NOT EXISTS nc_2_1_active_users_1h (
    "DATE" DATE,          -- 日期部分
    "USERID" VARCHAR(255), -- 用户 ID
    "total_duration" INT,       -- 活动持续时间（分钟）
);
INSERT INTO nc_2_1_active_users_1h
SELECT 
    "DATE",
    "USERID",
    "total_duration"

FROM 
    nc_2_0_user_stay_time
WHERE 
    "total_duration" >= 60;  -- 过滤累计活动时长至少为一小时的用户
DROP TABLE IF EXISTS nc_2_0_user_stay_time;

CREATE TABLE IF NOT EXISTS nc_2_0_user_stay_time (
    "USERID" VARCHAR(255),
    "DATE" DATE,
    total_duration FLOAT,
    total_duration_min FLOAT
);

INSERT INTO nc_2_0_user_stay_time ("USERID", "DATE", total_duration, total_duration_min)
SELECT 
    "USERID",
    "DATE",
    SUM("DURATION") AS total_duration,
    SUM("DURATION_MIN") AS total_duration_min
FROM 
    nc_user_stay_time_location
GROUP BY 
    "USERID",
    "DATE"
ORDER BY 
    "USERID",
    "DATE";
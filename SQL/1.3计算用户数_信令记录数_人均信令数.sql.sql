--## 3、计算用户总数（不重复的ID数目）、信令的总记录数、人均信令数。 
CREATE TABLE IF NOT EXISTS nc_1_3_user_signal_count (
    USERID VARCHAR(255),
    SIGNAL_COUNT INT8
);

INSERT INTO nc_1_3_user_signal_count (USERID, SIGNAL_COUNT)
SELECT "USERID", COUNT(*) AS SIGNAL_COUNT FROM nanchang_alldata GROUP BY "USERID";


SELECT COUNT(DISTINCT USERID) AS TOTAL_USERS FROM nc_1_3_user_signal_count

--用户数：1138713

SELECT SUM(signal_count) AS TOTAL_USERS FROM nc_1_3_user_signal_count
--记录数：298176438
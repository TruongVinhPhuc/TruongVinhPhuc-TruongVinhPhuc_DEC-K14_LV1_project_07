WITH user_cte AS (
    SELECT DISTINCT SAFE_CAST(user_id_db AS INT64) AS user_id,
        device_id AS device_id,
        email_address AS email,
    FROM raw_event.event 
    WHERE user_id_db != ''
    AND email_address != ''
)
SELECT * FROM user_cte
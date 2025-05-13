WITH store_cte AS (
    SELECT DISTINCT
        SAFE_CAST(store_id AS INT64) AS store_id,
        "Store " || store_id AS store_name
    FROM raw_event.event
    WHERE store_id IS NOT NULL
)
SELECT * FROM store_cte
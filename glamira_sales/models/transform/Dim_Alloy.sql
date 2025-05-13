WITH event AS (
    SELECT JSON_EXTRACT(option, '$') AS option_json
    FROM raw_event.event
    WHERE option IS NOT NULL
      AND collection IN (
        'add_to_cart_action',
        'select_product_option',
        'view_product_detail',
        'select_product_option_quality'
      )
),

unested AS (
    SELECT 
        SAFE_CAST(JSON_EXTRACT_SCALAR(option_item, '$.option_id') AS INT64) AS option_id,
        JSON_EXTRACT_SCALAR(option_item, '$.option_label') AS option_name,
        JSON_EXTRACT_SCALAR(option_item, '$.value_label') AS value_label,
        SAFE_CAST(JSON_EXTRACT_SCALAR(option_item, '$.value_id') AS INT64) AS value_id
    FROM event,
    UNNEST(JSON_EXTRACT_ARRAY(option_json)) AS option_item
    WHERE JSON_EXTRACT_SCALAR(option_item, '$.option_label') = 'alloy'
),

cart_products_option AS (
    SELECT 
        SAFE_CAST(opt.option_id AS INT64) AS option_id,
        opt.option_label AS option_name,
        opt.value_label AS value_label,
        SAFE_CAST(opt.value_id AS INT64) AS value_id
    FROM raw_event.event,
    UNNEST(cart_products) AS product,
    UNNEST(product.option) AS opt
    WHERE opt.option_label = 'alloy'
)

SELECT 
  {{ dbt_utils.generate_surrogate_key(['option_id', 'option_name', 'value_label', 'value_id']) }} AS alloy_id,
  *
FROM unested

UNION DISTINCT

SELECT 
  {{ dbt_utils.generate_surrogate_key(['option_id', 'option_name', 'value_label', 'value_id']) }} AS alloy_id,
  *
FROM cart_products_option

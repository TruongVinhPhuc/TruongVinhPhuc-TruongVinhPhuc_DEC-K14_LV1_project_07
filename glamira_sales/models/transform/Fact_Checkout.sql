WITH checkout_success AS (
    SELECT * 
    FROM raw_event.event
    WHERE collection = 'checkout_success'
),
fact_checkout AS (
    SELECT 
        SAFE_CAST(user_id_db AS INT64) AS user_id,
        {{ dbt_utils.generate_surrogate_key(["CAST(DATE(TIMESTAMP_SECONDS(time_stamp)) AS STRING)"]) }} AS date_id,
        {{ dbt_utils.generate_surrogate_key(['ip']) }} AS location_id,
        SAFE_CAST(product.product_id AS INT64) AS product_id,
        (
            SELECT {{ dbt_utils.generate_surrogate_key([
                'CAST(opt.option_id AS STRING)',
                'opt.option_label',
                'opt.value_label',
                'CAST(opt.value_id AS STRING)'
            ]) }}
            FROM UNNEST(product.option) AS opt
            WHERE opt.option_label = 'diamond'
        ) AS diamond_id,
        (
            SELECT {{ dbt_utils.generate_surrogate_key([
                'CAST(opt.option_id AS STRING)',
                'opt.option_label',
                'opt.value_label',
                'CAST(opt.value_id AS STRING)'
            ]) }}
            FROM UNNEST(product.option) AS opt
            WHERE opt.option_label = 'alloy'
        ) AS alloy_id,
        SAFE_CAST(store_id AS INT64) AS store_id,
        product.price,
        product.currency,
        product.amount,
        product.price * product.amount AS total,
        _id AS event_id,
    FROM checkout_success,
    UNNEST(cart_products) AS product
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['fact_checkout.event_id', 'fact_checkout.user_id', 'fact_checkout.date_id', 'fact_checkout.location_id', 'fact_checkout.product_id', 'fact_checkout.store_id', 'fact_checkout.diamond_id', 'fact_checkout.alloy_id']) }} AS fact_SK,
    fact_checkout.*
FROM fact_checkout
JOIN {{ ref('Dim_User') }} AS user ON fact_checkout.user_id = user.user_id
JOIN {{ ref('Dim_Date') }} AS date ON fact_checkout.date_id = date.date_id
JOIN {{ ref('Dim_Location') }} AS location ON fact_checkout.location_id = location.location_id
JOIN {{ ref('Dim_Product') }}AS product ON fact_checkout.product_id = product.product_id
JOIN {{ ref('Dim_Store') }} AS store ON fact_checkout.store_id = store.store_id
JOIN {{ ref('Dim_Alloy') }}AS alloy ON fact_checkout.alloy_id = alloy.alloy_id
JOIN {{ ref('Dim_Diamond') }} AS diamond ON fact_checkout.diamond_id = diamond.diamond_id

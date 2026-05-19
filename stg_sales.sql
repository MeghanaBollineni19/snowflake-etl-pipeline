-- dbt_models/stg_sales.sql
-- Staging model: clean and type-cast raw sales orders
-- This runs inside Snowflake as part of the dbt ELT layer

{{ config(
    materialized = 'incremental',
    unique_key   = 'order_id',
    cluster_by   = ['order_date', 'region']
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_sales_orders') }}
    {% if is_incremental() %}
        WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        order_id,
        TRY_TO_DATE(order_date, 'YYYY-MM-DD')         AS order_date,
        NULLIF(TRIM(customer_id), '')                  AS customer_id,
        INITCAP(TRIM(customer_name))                   AS customer_name,
        product_id,
        TRIM(product_name)                             AS product_name,
        UPPER(TRIM(category))                          AS category,
        TRY_TO_NUMBER(quantity)::NUMBER(10,0)          AS quantity,
        TRY_TO_NUMBER(unit_price)::NUMBER(18,4)        AS unit_price,
        TRY_TO_NUMBER(total_amount)::NUMBER(18,4)      AS total_amount,
        UPPER(TRIM(currency))                          AS currency,
        UPPER(TRIM(region))                            AS region,
        UPPER(TRIM(plant_code))                        AS plant_code,
        UPPER(TRIM(sap_document_type))                 AS sap_document_type,
        UPPER(TRIM(status))                            AS status,
        _pipeline_run_id,
        _ingested_at,
        CURRENT_TIMESTAMP()                            AS _transformed_at,
        CASE
            WHEN TRY_TO_DATE(order_date, 'YYYY-MM-DD') IS NULL THEN FALSE
            WHEN TRY_TO_NUMBER(quantity) IS NULL THEN FALSE
            WHEN TRY_TO_NUMBER(unit_price) <= 0 THEN FALSE
            ELSE TRUE
        END AS _is_valid
    FROM source
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM cleaned
)

SELECT * EXCLUDE rn
FROM deduped
WHERE rn = 1

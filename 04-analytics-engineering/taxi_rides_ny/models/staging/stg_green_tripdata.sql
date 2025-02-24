{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,
    row_number() over(partition by SAFE_CAST(vendorid AS INT64), lpep_pickup_datetime) as rn
  from {{ source('staging','green_tripdata') }}
  where vendorid is not null 
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,
    -- Cast vendorid to FLOAT64 for compatibility with the Parquet data schema
    {{ dbt.safe_cast("vendorid", api.Column.translate_type("integer")) }} as vendorid,
    {{ dbt.safe_cast("ratecodeid", api.Column.translate_type("integer")) }} as ratecodeid,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    CAST(lpep_pickup_datetime AS TIMESTAMP) as pickup_datetime,
    CAST(lpep_dropoff_datetime AS TIMESTAMP) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    SAFE_CAST(passenger_count AS FLOAT64) as passenger_count,
    CAST(trip_distance AS NUMERIC) as trip_distance,
    {{ dbt.safe_cast("trip_type", api.Column.translate_type("integer")) }} as trip_type,

    -- payment info
    CAST(fare_amount AS NUMERIC) as fare_amount,
    CAST(extra AS NUMERIC) as extra,
    CAST(mta_tax AS NUMERIC) as mta_tax,
    CAST(tip_amount AS NUMERIC) as tip_amount,
    CAST(tolls_amount AS NUMERIC) as tolls_amount,
    CAST(ehail_fee AS NUMERIC) as ehail_fee,
    CAST(improvement_surcharge AS NUMERIC) as improvement_surcharge,
    CAST(total_amount AS NUMERIC) as total_amount,
    COALESCE(SAFE_CAST(payment_type AS FLOAT64), 0) as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_type_description

from tripdata
where rn = 1

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}
  limit 100
{% endif %}

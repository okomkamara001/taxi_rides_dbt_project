{{ config(materialized='table') }}

with trips_data as (
    select * 
    from {{ ref('fact_trips') }}
)
select 
    -- Revenue grouping 
    pickup_zone as revenue_zone,  -- Pickup zone for revenue
    {{ dbt.date_trunc("month", "pickup_datetime") }} as revenue_month,  -- Monthly revenue truncation

    service_type,  -- Service type (Green/Yellow)

    -- Revenue calculation (sum of different amounts)
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthly_tolls_amount,
    sum(ehail_fee) as revenue_monthly_ehail_fee,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,

    -- Additional calculations
    count(tripid) as total_monthly_trips,  -- Count of trips per month
    avg(passenger_count) as avg_monthly_passenger_count,  -- Average passenger count per month
    avg(trip_distance) as avg_monthly_trip_distance  -- Average trip distance per month

from trips_data
-- Ensure you are grouping by these columns:
group by 
    revenue_zone, revenue_month, service_type;
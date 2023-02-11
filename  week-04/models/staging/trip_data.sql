{{ config(materialized="table") }}

  select *,
    row_number() over(partition by vendorid, tpep_pickup_datetime) as rn
  from {{ source('staging','nyc_trips') }}
  limit 10
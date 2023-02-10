{{ config(materialized="view") }}

select *
from {{ source('staging','nyc_trips') }}
limit 10

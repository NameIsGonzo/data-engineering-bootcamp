{{ config(materialized="view") }}

select *
from {{ source("staging", "green_trip_data") }}
limit 100

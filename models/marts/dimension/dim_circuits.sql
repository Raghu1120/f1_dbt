{{ config(materialized='table') }}

select
    circuit_id,

    circuit_ref,

    circuit_name as circuit_name,

    coalesce(location, 'unknown') as location,
    coalesce(country, 'unknown') as country,

    latitude,
    longitude,
    altitude

from {{ ref('stg_circuits') }}
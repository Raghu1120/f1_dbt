{{ config(materialized='table') }}

select
    r.race_id,

    r.year as season_year,
    r.round,

    r.race_name,

    r.race_date,
    r.race_time,
    r.race_datetime,

    c.circuit_id,
    c.circuit_name,
    c.location,
    c.country

from {{ ref('stg_races') }} r
left join {{ ref('stg_circuits') }} c
    on r.circuit_id = c.circuit_id
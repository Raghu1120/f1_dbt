{{ config(materialized='table') }}

select

    s.result_id,
    s.race_id,
    s.driver_id,
    s.constructor_id,
    s.status_id,

    r.year,
    r.round,
    r.circuit_id,
    r.race_name,
    r.race_date,

    s.grid,
    s.finish_position,
    s.points,
    s.laps,

    s.race_time,
    s.race_time_ms,
    s.fastestlap,
    s.fastest_lap_time,

    st.status as race_status,

    case when s.finish_position = 1 then 1 else 0 end as is_win,
    case when s.finish_position <= 3 then 1 else 0 end as is_podium,
    case when s.finish_position is null then 1 else 0 end as is_dnf

from {{ ref('stg_sprint_results') }} s

left join {{ ref('stg_races') }} r
    on s.race_id = r.race_id

left join {{ ref('stg_status') }} st
    on s.status_id = st.status_id
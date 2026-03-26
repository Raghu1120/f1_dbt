{{ config(materialized='table') }}

select

   
    r.result_id,
    r.race_id,
    r.driver_id,
    r.constructor_id,
    r.status_id,


    ra.year,
    ra.round,
    ra.circuit_id,
    ra.race_name,
    ra.race_date,

    
    r.grid,
    r.position,
    r.position_order,
    r.points,

    r.laps,
    r.milliseconds,
    r.fastest_lap,
    r.rank,

    
    lt.total_laps_recorded,
    lt.avg_lap_time_ms,
    lt.fastest_lap_time_ms as lap_fastest_ms,

   
    ps.total_pit_stops,
    ps.total_pit_duration_ms,

    q.position as qualifying_position,
    q.q1_time,
    q.q2_time,
    q.q3_time,


    s.status as race_status,

 
    case when r.position = 1 then 1 else 0 end as is_win,
    case when r.position <= 3 then 1 else 0 end as is_podium

from {{ ref('stg_results') }} r

left join {{ ref('stg_status') }} s
    on r.status_id = s.status_id

left join {{ ref('stg_races') }} ra
    on r.race_id = ra.race_id


left join (
    select
        race_id,
        driver_id,
        count(*) as total_laps_recorded,
        avg(lap_time_ms) as avg_lap_time_ms,
        min(lap_time_ms) as fastest_lap_time_ms
    from {{ ref('stg_lap_times') }}
    group by race_id, driver_id
) lt
    on r.race_id = lt.race_id
    and r.driver_id = lt.driver_id


left join (
    select
        race_id,
        driver_id,
        count(*) as total_pit_stops,
        sum(pit_duration_ms) as total_pit_duration_ms
    from {{ ref('stg_pit_stops') }}
    group by race_id, driver_id
) ps
    on r.race_id = ps.race_id
    and r.driver_id = ps.driver_id

left join {{ ref('stg_qualifying') }} q
    on r.race_id = q.race_id
    and r.driver_id = q.driver_id
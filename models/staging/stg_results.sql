{{ config(materialized='view') }}

with source as (

    select * 
    from {{ source('raw', 'results') }}

),

cleaned as (

    select
        resultid AS result_id,
        raceid as race_id,
        driverid as driver_id,
        constructorid as constructor_id,

        try_to_number(nullif(number::string, '\\N')) as number,
        try_to_number(nullif(grid::string, '\\N')) as grid,

        try_to_number(nullif(position::string, '\\N')) as position,
        nullif(positiontext::string, '\\N') as position_text,
        try_to_number(nullif(positionorder::string, '\\N')) as position_order,

        try_to_number(nullif(points::string, '\\N')) as points,
        try_to_number(nullif(laps::string, '\\N')) as laps,

        nullif(time::string, '\\N') as race_time,
        try_to_number(nullif(milliseconds::string, '\\N')) as milliseconds,

        try_to_number(nullif(fastestlap::string, '\\N')) as fastest_lap,
        try_to_number(nullif(rank::string, '\\N')) as rank,

        nullif(fastestlaptime::string, '\\N') as fastest_lap_time,
        try_to_double(nullif(fastestlapspeed::string, '\\N')) as fastest_lap_speed,

        try_to_number(nullif(statusid::string, '\\N')) as status_id

    from source

)

select * from cleaned
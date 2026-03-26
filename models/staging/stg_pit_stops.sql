{{ config(materialized='view') }}

SELECT
    raceid AS race_id,
    driverid AS driver_id,

    stop,
    lap,

    time AS pit_time,

    duration AS pit_duration,

    CAST(milliseconds AS INT) AS pit_duration_ms

FROM {{ source('raw', 'pit_stops') }}
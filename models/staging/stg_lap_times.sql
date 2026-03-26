{{ config(materialized='view') }}

SELECT
    raceid AS race_id,
    driverid AS driver_id,
    lap,

    CAST(position AS INT) AS position,

    time AS lap_time,

    CAST(milliseconds AS INT) AS lap_time_ms

FROM {{ source('raw', 'lap_times') }}
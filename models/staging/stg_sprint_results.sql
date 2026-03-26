{{ config(materialized='view') }}

SELECT
    resultid AS result_id,

    raceid AS race_id,
    driverid AS driver_id,
    constructorid AS constructor_id,

    number,
    grid,

    CAST(positionorder AS INT) AS finish_position,

    CASE 
        WHEN positionorder = 1 THEN 8
        WHEN positionorder = 2 THEN 7
        WHEN positionorder = 3 THEN 6
        WHEN positionorder = 4 THEN 5
        WHEN positionorder = 5 THEN 4
        WHEN positionorder = 6 THEN 3
        WHEN positionorder = 7 THEN 2
        WHEN positionorder = 8 THEN 1
        ELSE 0
    END AS points,

    laps,

    CASE 
        WHEN time = '\\N' THEN NULL
        ELSE time
    END AS race_time,

    CASE 
        WHEN milliseconds = '\\N' THEN NULL
        ELSE CAST(milliseconds AS INT)
    END AS race_time_ms,

    fastestlap,

    CASE 
        WHEN fastestlaptime = '\\N' THEN NULL
        ELSE fastestlaptime
    END AS fastest_lap_time,

    statusid AS status_id

FROM {{ source('raw', 'sprint_results') }}
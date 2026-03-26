{{ config(materialized='view') }}

SELECT
    qualifyid AS qualify_id,

    raceid AS race_id,
    driverid AS driver_id,
    constructorid AS constructor_id,

    number,
    position,

    CASE WHEN q1 = '\\N' THEN NULL ELSE q1 END AS q1_time,
    CASE WHEN q2 = '\\N' THEN NULL ELSE q2 END AS q2_time,
    CASE WHEN q3 = '\\N' THEN NULL ELSE q3 END AS q3_time

FROM {{ source('raw', 'qualifying') }}
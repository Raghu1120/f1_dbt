{{ config(materialized='view') }}

SELECT
    raceid AS race_id,

    year,
    round,

    circuitid AS circuit_id,

    TRIM(name) AS race_name,

    CAST(date AS DATE) AS race_date,

    CASE 
        WHEN time = '\\N' THEN NULL
        ELSE CAST(time AS TIME)
    END AS race_time,

    CASE 
        WHEN time = '\\N' THEN CAST(date AS TIMESTAMP)
        ELSE TO_TIMESTAMP(CONCAT(date, ' ', time))
    END AS race_datetime,

    url,

    CASE WHEN fp1_date = '\\N' THEN NULL ELSE fp1_date END AS fp1_date,
    CASE WHEN fp2_date = '\\N' THEN NULL ELSE fp2_date END AS fp2_date,
    CASE WHEN fp3_date = '\\N' THEN NULL ELSE fp3_date END AS fp3_date,
    CASE WHEN quali_date = '\\N' THEN NULL ELSE quali_date END AS qualifying_date,
    CASE WHEN sprint_date = '\\N' THEN NULL ELSE sprint_date END AS sprint_date

FROM {{ source('raw', 'races') }}
{{ config(materialized='view') }}

SELECT
    constructorresultsid AS constructor_results_id,

    raceid AS race_id,
    constructorid AS constructor_id,

    CAST(points AS FLOAT) AS points,

    CASE 
        WHEN status = '\\N' THEN NULL
        ELSE status
    END AS status

FROM {{ source('raw', 'constructor_results') }}
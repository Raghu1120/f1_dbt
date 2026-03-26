{{ config(materialized='view') }}

SELECT
    constructorstandingsid AS constructor_standings_id,

    raceid AS race_id,
    constructorid AS constructor_id,

    CAST(points AS FLOAT) AS points,

    CAST(position AS INT) AS position,

    CASE 
        WHEN positiontext = '\\N' THEN NULL
        ELSE positiontext
    END AS position_text,

    CAST(wins AS INT) AS wins

FROM {{ source('raw', 'constructor_standings') }}
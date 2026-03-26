{{ config(materialized='view') }}

SELECT
    circuitid AS circuit_id,

    circuitref AS circuit_ref,

    TRIM(name) AS circuit_name,
    TRIM(location) AS location,
    TRIM(country) AS country,

    UPPER(country) AS country_code,

    CAST(lat AS FLOAT) AS latitude,
    CAST(lng AS FLOAT) AS longitude,

    COALESCE(alt, 0) AS altitude,
    url

FROM {{ source('raw', 'circuits') }}
{{ config(materialized='view') }}

SELECT
    constructorid AS constructor_id,
    constructorref AS constructor_ref,

    TRIM(name) AS constructor_name,

    TRIM(nationality) AS nationality,
    UPPER(nationality) AS nationality_code,

    CONCAT(name, ' (', nationality, ')') AS constructor_full_name,

    url

FROM {{ source('raw', 'constructors') }}
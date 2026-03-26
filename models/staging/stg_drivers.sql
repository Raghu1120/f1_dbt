
{{ config(materialized='view') }}

SELECT
    driverid AS driver_id,
    TRIM(forename) AS first_name,
    TRIM(surname) AS last_name,
    CASE 
        WHEN number = '\\N' OR number IS NULL THEN NULL
        ELSE number
    END AS driver_number,
    CASE 
        WHEN code = '\\N' OR code IS NULL THEN 'unknown'
        ELSE code
    END AS driver_code,
    INITCAP(nationality) AS nationality,
    TRY_TO_DATE(dob) AS date_of_birth
FROM {{ source('raw', 'drivers') }}
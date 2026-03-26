{{ config(materialized='table') }}

select
    driver_id,

    first_name,
    last_name,

    concat(first_name, ' ', last_name) as full_name,

    coalesce(driver_number, -1) as driver_number,
    coalesce(driver_code, 'NA') as driver_code,

    nationality,

    date_of_birth,

    datediff(year, date_of_birth, current_date) as age

from {{ ref('stg_drivers') }}
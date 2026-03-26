{{ config(materialized='table') }}

select
    status_id,
    status

from {{ ref('stg_status') }}
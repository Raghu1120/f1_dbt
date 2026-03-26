{{config(materialized = 'view')}}

SELECT
STATUSID AS status_id,
status
FROM {{ source('raw', 'status') }}


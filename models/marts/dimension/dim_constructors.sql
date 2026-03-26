{{ config(materialized='table') }}

select
    constructor_id,

    constructor_ref,

    constructor_name,

    coalesce(constructor_full_name, constructor_name) as constructor_full_name,

    coalesce(nationality, 'unknown') as nationality

from {{ ref('stg_constructors') }}
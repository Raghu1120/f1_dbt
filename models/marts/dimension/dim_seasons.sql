{{ config(materialized='table') }}

select distinct
    season_year

from {{ ref('stg_seasons') }}
{{config(materialized = 'view')}}

select
    year as season_year,

    case 
        when url is null or url = '' then 'unknown'
        else url
    end as season_url

from {{ source('raw', 'seasons') }}
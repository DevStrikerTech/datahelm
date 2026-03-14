{{ config(materialized='view', tags=['daily', 'clashofclans']) }}

select
    id,
    name,
    cast(attackwins as integer) as attackwins,
    cast(defensewins as integer) as defensewins,
    achievements,
    troops,
    heroes,
    cast(last_mtime as timestamp) as last_mtime
from {{ source('raw_ingestion', 'clashofclans_stats') }}

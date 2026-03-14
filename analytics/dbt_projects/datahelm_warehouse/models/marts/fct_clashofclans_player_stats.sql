{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns',
    tags=['daily', 'clashofclans']
) }}

select
    id,
    name,
    attackwins,
    defensewins,
    achievements,
    troops,
    heroes,
    last_mtime
from {{ ref('stg_clashofclans_player_stats') }}
{% if is_incremental() %}
where last_mtime > (select coalesce(max(last_mtime), '1900-01-01'::timestamp) from {{ this }})
{% endif %}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set configured_schema = var('target_schema', target.schema) -%}
    {{ configured_schema | trim }}
{%- endmacro %}

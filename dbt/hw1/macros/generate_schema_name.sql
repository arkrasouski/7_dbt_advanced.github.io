{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set default_schema = target.schema -%}
  {%- if target.name == 'ci' -%}
    {{ return('dbt_ci_' ~ env_var('GITHUB_USER', 'unknown')) }}
  {%- elif custom_schema_name is none -%}
    {{ return(default_schema) }}
  {%- else -%}
    {{ return(custom_schema_name) }}
  {%- endif -%}
{%- endmacro %}
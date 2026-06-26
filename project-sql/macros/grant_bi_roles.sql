{# Otorga SELECT sobre los marts al rol de BI tras cada build (post-hook). #}
{% macro grant_bi_select() %}
    {% if target.name == 'prod' %}
        grant usage on schema {{ target.schema }} to role BI_READER;
        grant select on all tables in schema {{ target.schema }} to role BI_READER;
        grant select on future tables in schema {{ target.schema }} to role BI_READER;
    {% endif %}
{% endmacro %}

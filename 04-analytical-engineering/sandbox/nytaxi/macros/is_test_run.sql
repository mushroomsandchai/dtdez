{% macro is_test_run(test=true) %}
    {% if test %}
        limit 1000
    {% endif %}
{% endmacro %}
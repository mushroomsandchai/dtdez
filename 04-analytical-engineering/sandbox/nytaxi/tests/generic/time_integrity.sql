{% test time_integrity(model, start_time, end_time) %}
    select
        *
    from
        {{ model }}
    where
        {{ start_time }} > {{ end_time }}
{% endtest %}
    
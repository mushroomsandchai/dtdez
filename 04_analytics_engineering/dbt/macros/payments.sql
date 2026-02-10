-- Macro: payment_description()
-- Description: Generates a SQL query that creates a mapping table of payment types to their descriptions.
--              This macro uses a dictionary to define payment type codes (0-6) and their corresponding
--              descriptions, then constructs a UNION ALL query that returns all payment types with their labels.
-- 
-- Returns: A SELECT statement with two columns:
--          - payment_type (int): The numeric code for the payment type (0-6)
--          - payment_description (string): The human-readable description of the payment type
--
-- Payment Types:
--   0: Unknown
--   1: Credit card
--   2: Cash
--   3: No charge
--   4: Dispute
--   5: Unknown
--   6: Voided trip
--
-- Usage: {{ payment_description() }}
--
-- DEPENDENCIES:
--   None
--
-- Notes: The macro generates a separate SELECT statement for each payment type and combines them
--        using UNION ALL operators. This creates a reference table useful for fact or dimension tables.
{% macro payment_description() %}
    {% set payment_types = {
                                0: 'Unknown',
                                1: 'Credit card',
                                2: 'Cash',
                                3: 'No charge',
                                4: 'Dispute',
                                5: 'Unknown',
                                6: 'Voided trip'
                            }
    %}
    
    {% for type, description in payment_types.items() %}
        select
            cast({{ type }} as int) as payment_type,
            cast("{{ description }}" as string) as payment_description
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
{% endmacro %}
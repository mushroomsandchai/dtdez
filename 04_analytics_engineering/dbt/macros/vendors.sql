-- =====================================================================
-- MACRO: vendor_name()
-- =====================================================================
-- DESCRIPTION:
--   Generates a mapping table of vendor IDs to vendor names.
--   This macro creates a SQL query that returns a two-column result set
--   containing vendor identifiers and their corresponding names.
--
-- RETURNS:
--   A query result set with the following columns:
--   - vendor_id (INTEGER): Unique identifier for the vendor
--   - vendor_name (STRING): Name/description of the vendor
--
-- DETAILS:
--   The macro iterates through a predefined dictionary of vendor mappings
--   and constructs a UNION ALL query combining all vendor records.
--   Vendor IDs: 0 (Unknown), 1 (Creative Mobile Technologies),
--              2 (VeriFone Inc.), 4 (Unknown/Other)
--
-- USAGE:
--   SELECT * FROM {{ vendor_name() }}
--
-- DEPENDENCIES:
--   None
--
-- =====================================================================
{% macro vendor_name() %}
    {% set vendros = {
                        0: 'Unknown',
                        1: 'Creative Mobile Technologies',
                        2: 'VeriFone Inc.',
                        4: 'Unknown/Other'
                    }
    %}
    
    {% for id, name in vendros.items() %}
        select
            cast({{ id }} as integer) as vendor_id,
            cast("{{ name }}" as string) as vendor_name
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}
{% endmacro %}
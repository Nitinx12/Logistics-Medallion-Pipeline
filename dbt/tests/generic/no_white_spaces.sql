{% test no_white_spaces(model, column_name) %}

with validation_errors as (

    select
        {{ column_name }} as space_column
    from {{ model }}
    where 
        -- Catches leading white spaces
        {{ column_name }} like ' %' 
        -- Catches trailing white spaces
        or {{ column_name }} like '% '
        -- Catches consecutive internal white spaces (optional)
        or {{ column_name }} like '%  %'

)

select *
from validation_errors

{% endtest %}

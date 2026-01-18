{{
    config(
        materialized="view",
        alias="example_model_csv"
    )
}}

select
    *
from {{ source_onelake('anlb', 'ANLb_Beheerpakket_Indeling_csv') }}

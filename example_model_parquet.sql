{{
    config(
        materialized="view",
        alias="example_model_parquet"
    )
}}

select
    *
from {{ source_onelake('anlb', 'ANLb_Beheerpakket_Indeling_Parquet') }}

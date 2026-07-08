-- Staging: clean, rename, and type the raw CMS Part D prescriber feed.
-- One row per (npi, generic_name). Views only — cheap to rebuild.

with source as (

    select * from {{ source('raw', 'part_d_prescribers') }}

),

renamed as (

    select
        cast(Prscrbr_NPI as varchar)                    as npi,
        upper(substr(trim(Prscrbr_Last_Org_Name), 1, 1))
            || lower(substr(trim(Prscrbr_Last_Org_Name), 2)) as prescriber_last_name,
        upper(substr(trim(Prscrbr_First_Name), 1, 1))
            || lower(substr(trim(Prscrbr_First_Name), 2))    as prescriber_first_name,
        upper(trim(Prscrbr_State_Abrvtn))               as state,
        trim(Prscrbr_Type)                              as prescriber_type,
        trim(Brnd_Name)                                 as brand_name,
        trim(Gnrc_Name)                                 as generic_name,
        cast(Tot_Clms as bigint)                        as total_claims,
        cast(Tot_Day_Suply as bigint)                   as total_day_supply,
        cast(Tot_Drug_Cst as double)                    as total_drug_cost,
        _ingest_date

    from source

),

filtered as (

    select
        *,
        round(total_drug_cost / nullif(total_claims, 0), 2) as cost_per_claim
    from renamed
    where npi is not null
      and total_claims > 0

)

select * from filtered

-- Mart: state-level drug spend and prescribing intensity.
-- Answers: which states spend the most, on which drugs, at what cost/claim?

with prescribers as (

    select * from {{ ref('stg_prescribers') }}

),

state_drug as (

    select
        state,
        generic_name,
        count(distinct npi)                         as prescriber_count,
        sum(total_claims)                           as total_claims,
        sum(total_drug_cost)                        as total_drug_cost,
        round(sum(total_drug_cost)
              / nullif(sum(total_claims), 0), 2)    as avg_cost_per_claim

    from prescribers
    group by 1, 2

),

ranked as (

    select
        *,
        row_number() over (
            partition by state
            order by total_drug_cost desc
        ) as spend_rank_in_state

    from state_drug

)

select * from ranked

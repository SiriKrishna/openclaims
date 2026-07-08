-- Mart: highest-volume prescribers with cost outlier flag.
-- Outlier flag = cost/claim more than 3x the national average for that drug.

with prescribers as (

    select * from {{ ref('stg_prescribers') }}

),

drug_benchmarks as (

    select
        generic_name,
        avg(cost_per_claim) as natl_avg_cost_per_claim
    from prescribers
    group by 1

),

joined as (

    select
        p.npi,
        p.prescriber_last_name,
        p.prescriber_first_name,
        p.state,
        p.prescriber_type,
        p.generic_name,
        p.total_claims,
        p.total_drug_cost,
        p.cost_per_claim,
        b.natl_avg_cost_per_claim,
        p.cost_per_claim > 3 * b.natl_avg_cost_per_claim as is_cost_outlier

    from prescribers p
    left join drug_benchmarks b using (generic_name)

)

select * from joined

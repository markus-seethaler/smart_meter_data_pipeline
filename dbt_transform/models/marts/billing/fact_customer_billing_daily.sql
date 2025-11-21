{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['billing_date', 'customer_id'], 'type': 'btree'},
            {'columns': ['customer_id'], 'type': 'btree'},
            {'columns': ['billing_date'], 'type': 'btree'}
        ]
    )
}}

with meter_readings as (
    select * from {{ ref('stg_meter_readings') }}
),

meters as (
    select * from {{ source('smart_meter', 'dim_meters') }}
),

customers as (
    select * from {{ source('smart_meter', 'dim_customers') }}
),

-- Aggregate readings to daily level per meter
daily_meter_totals as (
    select
        date_trunc('day', reading_timestamp)::date as billing_date,
        meter_id,

        -- Total consumption and production for the day (sum of kWh deltas)
        sum(consumption_kwh) as total_consumption_kwh,
        sum(production_kwh) as total_production_kwh,
        sum(net_consumption_kwh) as total_net_consumption_kwh,

        -- Reading counts and quality metrics
        count(*) as reading_count,
        sum(case when is_valid then 1 else 0 end) as valid_reading_count,
        sum(case when is_estimated then 1 else 0 end) as estimated_reading_count,
        sum(case when is_error then 1 else 0 end) as error_reading_count,

        -- Solar flag
        max(has_solar::int)::boolean as has_solar

    from meter_readings
    group by 1, 2
),

-- Aggregate to customer level
daily_customer_totals as (
    select
        dmt.billing_date,
        m.customer_id,
        c.customer_name,

        -- Aggregate across all customer meters
        sum(dmt.total_consumption_kwh) as total_consumption_kwh,
        sum(dmt.total_production_kwh) as total_production_kwh,
        sum(dmt.total_net_consumption_kwh) as total_net_consumption_kwh,

        -- Meter and reading counts
        count(distinct dmt.meter_id) as meter_count,
        sum(dmt.reading_count) as total_reading_count,
        sum(dmt.valid_reading_count) as valid_reading_count,
        sum(dmt.estimated_reading_count) as estimated_reading_count,
        sum(dmt.error_reading_count) as error_reading_count,

        -- Solar flag (true if any meter has solar)
        max(dmt.has_solar::int)::boolean as has_solar

    from daily_meter_totals dmt
    inner join meters m on dmt.meter_id = m.meter_id
    inner join customers c on m.customer_id = c.customer_id
    group by 1, 2, 3
),

-- Calculate billing amounts
final as (
    select
        billing_date,
        customer_id,
        customer_name,

        -- Energy totals
        total_consumption_kwh,
        total_production_kwh,
        total_net_consumption_kwh,

        -- Calculate charges using standard residential rate ($0.28 per kWh)
        -- Charge for consumption
        total_consumption_kwh * 0.28 as consumption_charge,

        -- Credit for solar production (at same rate)
        total_production_kwh * 0.28 as production_credit,

        -- Net charge (consumption minus solar credit)
        total_net_consumption_kwh * 0.28 as net_charge,

        -- Metrics
        meter_count,
        total_reading_count,
        valid_reading_count,
        estimated_reading_count,
        error_reading_count,
        has_solar,

        -- Data quality percentage
        case
            when total_reading_count > 0
            then (valid_reading_count::float / total_reading_count::float) * 100
            else 0
        end as data_quality_pct

    from daily_customer_totals
)

select * from final

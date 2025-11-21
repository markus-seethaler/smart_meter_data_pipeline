{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('smart_meter', 'raw_meter_readings') }}
),

-- Calculate deltas from cumulative readings using LAG window function
with_previous_readings as (
    select
        reading_timestamp,
        meter_id,
        reading_consumption_milliwatts as cumulative_consumption_mwh,
        reading_production_milliwatts as cumulative_production_mwh,
        status,
        arrived_at,

        -- Get previous cumulative reading for this meter
        lag(reading_consumption_milliwatts) over (
            partition by meter_id
            order by reading_timestamp
        ) as prev_cumulative_consumption_mwh,

        lag(reading_production_milliwatts) over (
            partition by meter_id
            order by reading_timestamp
        ) as prev_cumulative_production_mwh

    from source
),

cleaned as (
    select
        reading_timestamp,
        meter_id,
        cumulative_consumption_mwh,
        cumulative_production_mwh,
        status,
        arrived_at,

        -- Calculate period consumption (delta = current - previous cumulative)
        -- For first reading, use cumulative value as delta
        coalesce(
            cumulative_consumption_mwh - prev_cumulative_consumption_mwh,
            cumulative_consumption_mwh
        ) as consumption_delta_mwh,

        coalesce(
            cumulative_production_mwh - prev_cumulative_production_mwh,
            cumulative_production_mwh
        ) as production_delta_mwh,

        -- Convert milliwatt-hours to kilowatt-hours (1 kWh = 1,000,000 mWh)
        coalesce(
            cumulative_consumption_mwh - prev_cumulative_consumption_mwh,
            cumulative_consumption_mwh
        ) / 1000000.0 as consumption_kwh,

        coalesce(
            cumulative_production_mwh - prev_cumulative_production_mwh,
            cumulative_production_mwh
        ) / 1000000.0 as production_kwh,

        -- Net consumption (consumption - production)
        (coalesce(
            cumulative_consumption_mwh - prev_cumulative_consumption_mwh,
            cumulative_consumption_mwh
        ) - coalesce(
            coalesce(cumulative_production_mwh - prev_cumulative_production_mwh, cumulative_production_mwh),
            0
        )) / 1000000.0 as net_consumption_kwh,

        -- Data quality flags
        case when status = 'V' then true else false end as is_valid,
        case when status = 'E' then true else false end as is_estimated,
        case when status = 'R' then true else false end as is_error,

        -- Identify solar meters
        case when cumulative_production_mwh is not null then true else false end as has_solar

    from with_previous_readings
)

select * from cleaned

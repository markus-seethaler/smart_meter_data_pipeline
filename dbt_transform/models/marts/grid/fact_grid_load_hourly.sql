{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['load_hour', 'grid_zone_id'], 'type': 'btree'},
            {'columns': ['grid_zone_id'], 'type': 'btree'},
            {'columns': ['load_hour'], 'type': 'btree'}
        ]
    )
}}

with meter_readings as (
    select * from {{ ref('stg_meter_readings') }}
),

meters as (
    select * from {{ source('smart_meter', 'dim_meters') }}
),

grid_zones as (
    select * from {{ source('smart_meter', 'dim_grid_zones') }}
),

-- Aggregate readings to hourly level per meter
hourly_meter_totals as (
    select
        date_trunc('hour', reading_timestamp) as load_hour,
        meter_id,

        -- Total consumption and production for the hour (sum of kWh deltas)
        sum(consumption_kwh) as total_consumption_kwh,
        sum(production_kwh) as total_production_kwh,
        sum(net_consumption_kwh) as total_net_consumption_kwh,

        -- Reading counts
        count(*) as reading_count,
        sum(case when is_valid then 1 else 0 end) as valid_reading_count

    from meter_readings
    group by 1, 2
),

-- Aggregate to grid zone level
hourly_grid_zone_totals as (
    select
        hmt.load_hour,
        m.grid_zone_id,
        gz.zone_name,
        gz.zone_type,
        gz.max_capacity_megawatts,

        -- Aggregate across all meters in the zone
        sum(hmt.total_consumption_kwh) as total_consumption_kwh,
        sum(hmt.total_production_kwh) as total_production_kwh,
        sum(hmt.total_net_consumption_kwh) as total_net_consumption_kwh,

        -- Meter counts
        count(distinct hmt.meter_id) as active_meter_count,
        sum(hmt.reading_count) as total_reading_count,
        sum(hmt.valid_reading_count) as valid_reading_count

    from hourly_meter_totals hmt
    inner join meters m on hmt.meter_id = m.meter_id
    inner join grid_zones gz on m.grid_zone_id = gz.grid_zone_id
    group by 1, 2, 3, 4, 5
),

-- Calculate load metrics
final as (
    select
        load_hour,
        grid_zone_id,
        zone_name,
        zone_type,
        max_capacity_megawatts,

        -- Energy totals (kWh consumed during this hour)
        total_consumption_kwh,
        total_production_kwh,
        total_net_consumption_kwh,

        -- Convert to megawatt-hours for capacity comparison
        total_net_consumption_kwh / 1000.0 as total_net_consumption_mwh,

        -- Calculate capacity utilization (energy vs max capacity * 1 hour)
        case
            when max_capacity_megawatts > 0
            then ((total_net_consumption_kwh / 1000.0) / max_capacity_megawatts) * 100
            else 0
        end as capacity_utilization_pct,

        -- Peak load flag (over 80% capacity)
        case
            when max_capacity_megawatts > 0 and ((total_net_consumption_kwh / 1000.0) / max_capacity_megawatts) > 0.8
            then true
            else false
        end as is_peak_load,

        -- Critical load flag (over 95% capacity)
        case
            when max_capacity_megawatts > 0 and ((total_net_consumption_kwh / 1000.0) / max_capacity_megawatts) > 0.95
            then true
            else false
        end as is_critical_load,

        -- Metrics
        active_meter_count,
        total_reading_count,
        valid_reading_count,

        -- Data quality percentage
        case
            when total_reading_count > 0
            then (valid_reading_count::float / total_reading_count::float) * 100
            else 0
        end as data_quality_pct

    from hourly_grid_zone_totals
)

select * from final

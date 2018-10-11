INSERT INTO vw_regions_entries_exits_hourly (
user_journey_ID,
    user_id,
    local_month,
    travel_month,
    EntryType,
    gz_id,
    entry_time,
    in_region_datetime1,
    ir_time_of_day,
    in_region_hour,
    in_region_datetime2,
    exit_time,
    entered_from_region,
    tourism_region_name,
    went_to_region,
    latitude,
    longitude,
    Prior_latitude,
    Prior_longitude,
    Next_latitude,
    Next_longitude,
    state,
    crossing_in_duration,
    crossing_out_duration,
    time_in_region_hours,
    active,
    country,
    Source)
SELECT user_journey_ID,
    user_id,
    local_month,
    travel_month,
    EntryType,
    gz_id,
    entry_time,
    in_region_datetime1,
    ir_time_of_day,
    in_region_hour,
    in_region_datetime2,
    exit_time,
    entered_from_region,
    tourism_region_name,
    went_to_region,
    latitude,
    longitude,
    Prior_latitude,
    Prior_longitude,
    Next_latitude,
    Next_longitude,
    state,
    crossing_in_duration,
    crossing_out_duration,
    time_in_region_hours,
    active,
    country,
    Source
FROM {source}
CREATE TABLE IF NOT EXISTS vw_regions_entries_exits_hourly (
user_journey_ID text,
user_id text,
local_month text,
travel_month text,
EntryType text,
gz_id text,
entry_time text,
in_region_datetime1 text,
ir_time_of_day text,
in_region_hour text,
in_region_datetime2 text,
exit_time text,
entered_from_region text,
tourism_region_name text,
went_to_region text,
latitude text,
longitude text,
Prior_latitude text,
Prior_longitude text,
Next_latitude text,
Next_longitude text,
state text,
crossing_in_duration text,
crossing_out_duration text,
time_in_region_hours text,
active text,
country text,
Source text)

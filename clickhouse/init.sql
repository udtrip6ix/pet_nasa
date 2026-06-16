CREATE DATABASE IF NOT EXISTS opensky;

CREATE TABLE IF NOT EXISTS opensky.flights (
    icao24 String,
    callsign String,
    origin_country String,
    time_position UInt32,
    last_contact UInt32,
    longitude Float64,
    latitude Float64,
    baro_altitude Float64,
    updated_at DateTime DEFAULT now() 
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (icao24, last_contact);
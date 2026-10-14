-- Minimal gold schema for the site build tests.
create schema main_gold;
create table main_gold.fct_flight as select * from (values
  ('k1', 'RYR', 'EIDW', 'on_time', true), ('k2', 'RYR', 'EIDW', '180_plus', true),
  ('k3', 'EIN', 'LEPA', 'cancelled', false)) t(flight_key, airline_icao, origin_icao, delay_band, has_arrived);
create table main_gold.fct_eu261_claim as select * from (values
  ('FR1', 'EIDW', 'LEMD', 'arrival_delay', 200, 250.0, true, 250.0, 0.0, 'thunderstorm'),
  ('EI2', 'EIDW', 'KJFK', 'cancellation', null, 600.0, true, 0.0, 600.0, '')
) t(flight_number, origin_icao, destination_icao, disruption_kind, arrival_delay_minutes, amount_eur,
    is_eligible, likely_exempt_eur, expected_payout_eur, severe_weather);
alter table main_gold.fct_eu261_claim add column extraordinary_circumstances_likely boolean default false;
create table main_gold.fct_aircraft_position as select * from (values (-6.2, 53.4, false), (2.5, 49.0, true))
  t(longitude, latitude, on_ground);
create table main_gold.agg_eu261_exposure_by_airline as select * from (values
  ('RYR', 'Ryanair', 2, 0.0, 250.0), ('EIN', 'Aer Lingus', 1, 600.0, 0.0))
  t(airline_icao, airline_name, flights, expected_payout_eur, likely_exempt_eur);
create table main_gold.agg_airport_daily_delays as select * from (values
  ('EIDW', date '2026-10-14', 2, 45.0, 0.5), ('LEPA', date '2026-10-14', 1, 10.0, null))
  t(airport_icao, flight_date, departures, avg_departure_delay_minutes, on_time_rate);
create table main_gold.dim_airport as select * from (values
  ('EIDW', 'Dublin Airport', -6.27, 53.42), ('LEPA', 'Palma', 2.74, 39.55))
  t(airport_icao, airport_name, longitude, latitude);

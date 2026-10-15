-- Minimal gold tables for the gold quality suites.
create schema main_gold;
create table main_gold.fct_flight as
    select 'k' || i as flight_key, i = 0 as is_cancelled,
           10 as departure_delay_minutes, 12 as arrival_delay_minutes
    from range(10) t(i);
create table main_gold.fct_eu261_claim as
    select 'c1' as claim_id, 250 as amount_eur, 'k1' as flight_key;
create table main_gold.agg_airport_daily_delays as
    select 0.8 as on_time_rate, 4 as departures;

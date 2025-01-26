-- .----------------------------------------------------------------------------------------------------------------------.
-- |░█▀▄░█▀▀░█░█░█▀▀░█░░░█▀█░█▀█░█▄█░█▀▀░█▀█░▀█▀░░░█▀▄░█░█░░░█▀▄░█▀█░█▄█░█▀▀░█▀▄░▀█▀░▀█▀░█▀█░░░█▄█░█▀█░█▀▄░█▀█░▀█▀░█▀▀    |
-- |░█░█░█▀▀░▀▄▀░█▀▀░█░░░█░█░█▀▀░█░█░█▀▀░█░█░░█░░░░█▀▄░░█░░░░█▀▄░█░█░█░█░█▀▀░█▀▄░░█░░░█░░█░█░░░█░█░█░█░█▀▄░█▀█░░█░░▀▀█    |
-- |░▀▀░░▀▀▀░░▀░░▀▀▀░▀▀▀░▀▀▀░▀░░░▀░▀░▀▀▀░▀░▀░░▀░░░░▀▀░░░▀░░░░▀░▀░▀▀▀░▀░▀░▀▀▀░▀░▀░▀▀▀░░▀░░▀▀▀░░░▀░▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀    |
-- |https://www.linkedin.com/in/romeritomorais/                                                                           |
-- '----------------------------------------------------------------------------------------------------------------------'
select
    aircraft_damage,
    aircraft_fate,
    cn_msn,
    collision_casualties,
    crash_site_elevation,
    crew_fatalities,
    crew_occupants,
    cycles,
    date,
    split(date," ")[0] AS date_of_week,
    split(date," ")[1] AS date_of_day,
    split(date," ")[2] AS date_of_month,
    split(date," ")[3] AS date_of_year,
    departure_airport,
    departure_airport_name,
    departure_airport_country,
    destination_airport,
    engines,
    first_flight,
    flightnumber,
    ground_casualties,
    leased_from,
    location,
    nature,
    operator,
    passengers_fatalities,
    passengers_occupants,
    phase,
    registration,
    status,
    time,
    type
from table_aviation_extract_date
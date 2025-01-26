-- .----------------------------------------------------------------------------------------------------------------------.
-- |░█▀▄░█▀▀░█░█░█▀▀░█░░░█▀█░█▀█░█▄█░█▀▀░█▀█░▀█▀░░░█▀▄░█░█░░░█▀▄░█▀█░█▄█░█▀▀░█▀▄░▀█▀░▀█▀░█▀█░░░█▄█░█▀█░█▀▄░█▀█░▀█▀░█▀▀    |
-- |░█░█░█▀▀░▀▄▀░█▀▀░█░░░█░█░█▀▀░█░█░█▀▀░█░█░░█░░░░█▀▄░░█░░░░█▀▄░█░█░█░█░█▀▀░█▀▄░░█░░░█░░█░█░░░█░█░█░█░█▀▄░█▀█░░█░░▀▀█    |
-- |░▀▀░░▀▀▀░░▀░░▀▀▀░▀▀▀░▀▀▀░▀░░░▀░▀░▀▀▀░▀░▀░░▀░░░░▀▀░░░▀░░░░▀░▀░▀▀▀░▀░▀░▀▀▀░▀░▀░▀▀▀░░▀░░▀▀▀░░░▀░▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀    |
-- |https://www.linkedin.com/in/romeritomorais/                                                                           |
-- '----------------------------------------------------------------------------------------------------------------------'
WITH table_json AS (
    SELECT
        json_table.*
    FROM
        table_aviation_select_structure
),
table_column_renamed AS (
    SELECT
        `Aircraft damage:`           AS aircraft_damage
        ,`Aircraft fate:`             AS aircraft_fate
        ,`C/n / msn:`                 AS cn_msn
        ,`Collision casualties:`      AS collision_casualties
        ,`Crash site elevation:`      AS crash_site_elevation
        ,`Crew:`                      AS crew
        ,`Cycles:`                    AS cycles
        ,`Date:`                      AS date
        ,`Departure airport:`         AS departure_airport
        ,`Destination airport:`       AS destination_airport
        ,`Engines:`                   AS engines
        ,`First flight:`              AS first_flight
        ,`Flightnumber:`              AS flightnumber
        ,`Ground casualties:`         AS ground_casualties
        ,`Leased from:`               AS leased_from
        ,`Location:`                  AS location
        ,`Nature:`                    AS nature
        ,`On behalf of:`              AS on_behalf_of
        ,`Operated by:`               AS operated_by
        ,`Operating for:`             AS operating_for
        ,`Operator:`                  AS operator
        ,`Passengers:`                AS passengers
        ,`Phase:`                     AS phase
        ,`Registration:`              AS registration
        ,`Status:`                    AS status
        ,`Time:`                      AS time
        ,`Total airframe hrs:`        AS total_airframe_hrs
        ,`Total:`                     AS total
        ,`Type:`                      AS type
        ,`json_cleaned`               AS json_cleaned
    FROM
        table_json
),
table_transform_crew AS (
    SELECT
        aircraft_damage,
        aircraft_fate,
        cn_msn,
        collision_casualties,
        crash_site_elevation,
        SPLIT(SPLIT(crew, '/')[0], ':')[1] AS crew_fatalities,
        SPLIT(SPLIT(crew, '/')[1], ':')[1] AS crew_occupants,
        cycles,
        date,
        departure_airport,
        TRIM(SPLIT(departure_airport, ',')[0]) AS departure_airport_name,
        TRIM(SPLIT(departure_airport, ',')[1]) AS departure_airport_country,
        destination_airport,
        engines,
        first_flight,
        flightnumber,
        ground_casualties,
        leased_from,
        location,
        nature,
        operator,
        SPLIT(SPLIT(passengers, '/')[0], ':')[1] AS passengers_fatalities,
        SPLIT(SPLIT(passengers, '/')[1], ':')[1] AS passengers_occupants,
        phase,
        registration,
        status,
        time,
        type
    FROM
        table_column_renamed
)

SELECT
    *
FROM
    table_transform_crew
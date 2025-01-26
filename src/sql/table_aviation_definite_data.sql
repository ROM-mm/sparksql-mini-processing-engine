-- .----------------------------------------------------------------------------------------------------------------------.
-- |░█▀▄░█▀▀░█░█░█▀▀░█░░░█▀█░█▀█░█▄█░█▀▀░█▀█░▀█▀░░░█▀▄░█░█░░░█▀▄░█▀█░█▄█░█▀▀░█▀▄░▀█▀░▀█▀░█▀█░░░█▄█░█▀█░█▀▄░█▀█░▀█▀░█▀▀    |
-- |░█░█░█▀▀░▀▄▀░█▀▀░█░░░█░█░█▀▀░█░█░█▀▀░█░█░░█░░░░█▀▄░░█░░░░█▀▄░█░█░█░█░█▀▀░█▀▄░░█░░░█░░█░█░░░█░█░█░█░█▀▄░█▀█░░█░░▀▀█    |
-- |░▀▀░░▀▀▀░░▀░░▀▀▀░▀▀▀░▀▀▀░▀░░░▀░▀░▀▀▀░▀░▀░░▀░░░░▀▀░░░▀░░░░▀░▀░▀▀▀░▀░▀░▀▀▀░▀░▀░▀▀▀░░▀░░▀▀▀░░░▀░▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀    |
-- |https://www.linkedin.com/in/romeritomorais/                                                                           |
-- '----------------------------------------------------------------------------------------------------------------------'

WITH aviation_transform_date_number AS (
    SELECT
        *,
        CASE
            WHEN date_of_month = 'September' THEN '09'
            WHEN date_of_month = 'October' THEN '10'
            WHEN date_of_month = 'November' THEN '11'
            WHEN date_of_month = 'May' THEN '05'
            WHEN date_of_month = 'March' THEN '03'
            WHEN date_of_month = 'June' THEN '06'
            WHEN date_of_month = 'July' THEN '07'
            WHEN date_of_month = 'January' THEN '01'
            WHEN date_of_month = 'December' THEN '12'
            WHEN date_of_month = 'August' THEN '08'
            WHEN date_of_month = 'April' THEN '04'
            WHEN date_of_month = 'February' THEN '02'
        END AS date_of_month1,
        CASE
            WHEN LENGTH(date_of_day) = 1 THEN CONCAT('0', date_of_day)
            ELSE date_of_day
        END AS date_of_day1
    FROM table_aviation_definite_data
),

-- Criando a coluna final "date" no formato 'YYYY-MM-DD'
aviation_transform_date AS (
    SELECT
        aircraft_damage,
        aircraft_fate,
        cn_msn,
        collision_casualties,
        crash_site_elevation,
        crew_fatalities,
        crew_occupants,
        cycles,
        CONCAT_WS('-', date_of_year, date_of_month1, date_of_day1) AS date,
        date_of_week,
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
    FROM aviation_transform_date_number
)

-- Selecionando o resultado final com as colunas auxiliares removidas
SELECT
    *
FROM
    aviation_transform_date;
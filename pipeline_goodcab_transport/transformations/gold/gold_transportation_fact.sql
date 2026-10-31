
CREATE OR REFRESH MATERIALIZED VIEW IDENTIFIER(:catalog || '.gold.gold_city_trips_fact') AS
SELECT
    c.city_id,
    c.city_name,
    t.trip_id,
    t.date,
    t.passenger_type,
    t.start_time,
    t.end_time,
    t.distance_travelled_km,
    t.fare_amount,
    t.passenger_rating,
    t.driver_rating,
    t.duration,
    t.bike_id,

    cal.id AS date_id,
    cal.year,
    cal.month,
    cal.month_name,
    cal.day_of_month,
    cal.day_name,
    cal.day_of_week,
    cal.week_of_year,
    cal.quarter,
    cal.is_weekend,
    cal.is_holiday,
    cal.holiday_name
FROM IDENTIFIER(:catalog || '.silver.silver_trips') t
LEFT JOIN IDENTIFIER(:catalog || '.silver.silver_city') c
    ON c.city_id = t.city_id
LEFT JOIN IDENTIFIER(:catalog || '.silver.calender') cal
    ON CAST(t.date AS DATE) = cal.date
;

CREATE OR REFRESH MATERIALIZED VIEW IDENTIFIER(:catalog || '.gold.gold_city_trips_jaipur_fact') AS
SELECT *
FROM IDENTIFIER(:catalog || '.gold.gold_city_trips_fact')
WHERE city_id = 'RJ01';

CREATE OR REFRESH MATERIALIZED VIEW IDENTIFIER(:catalog || '.gold.gold_city_trips_lucknow_fact') AS
SELECT *
FROM IDENTIFIER(:catalog || '.gold.gold_city_trips_fact')
WHERE city_id = 'UP01';


CREATE OR REFRESH MATERIALIZED VIEW IDENTIFIER(:catalog || '.gold.gold_city_trips_surat_fact') AS
SELECT *
FROM IDENTIFIER(:catalog || '.gold.gold_city_trips_fact')
WHERE city_id = 'GJ01';


CREATE OR REFRESH MATERIALIZED VIEW IDENTIFIER(:catalog || '.gold.gold_city_trips_kochi_fact') AS
SELECT *
FROM IDENTIFIER(:catalog || '.gold.gold_city_trips_fact')
WHERE city_id = 'KL01';


CREATE OR REFRESH MATERIALIZED VIEW IDENTIFIER(:catalog || '.gold.gold_city_trips_indore_fact') AS
SELECT *
FROM IDENTIFIER(:catalog || '.gold.gold_city_trips_fact')
WHERE city_id = 'MP01';


CREATE OR REFRESH MATERIALIZED VIEW IDENTIFIER(:catalog || '.gold.gold_city_trips_chandigarh_fact') AS
SELECT *
FROM IDENTIFIER(:catalog || '.gold.gold_city_trips_fact')
WHERE city_id = 'CH01';


CREATE OR REFRESH MATERIALIZED VIEW IDENTIFIER(:catalog || '.gold.gold_city_trips_vadodara_fact') AS
SELECT *
FROM IDENTIFIER(:catalog || '.gold.gold_city_trips_fact')
WHERE city_id = 'GJ02';


CREATE OR REFRESH MATERIALIZED VIEW IDENTIFIER(:catalog || '.gold.gold_city_trips_visakhapatnam_fact') AS
SELECT *
FROM IDENTIFIER(:catalog || '.gold.gold_city_trips_fact')
WHERE city_id = 'AP01';


CREATE OR REFRESH MATERIALIZED VIEW IDENTIFIER(:catalog || '.gold.gold_city_trips_coimbatore_fact') AS
SELECT *
FROM IDENTIFIER(:catalog || '.gold.gold_city_trips_fact')
WHERE city_id = 'TN01';


CREATE OR REFRESH MATERIALIZED VIEW IDENTIFIER(:catalog || '.gold.gold_city_trips_mysore_fact') AS
SELECT *
FROM IDENTIFIER(:catalog || '.gold.gold_city_trips_fact')
WHERE city_id = 'KA01';



<<<<<<< HEAD
-- Drop the table if it exists
DROP TABLE IF EXISTS tbl_analytics;

-- Create the table with the desired structure and data
CREATE TABLE tbl_analytics AS 
SELECT 
    f.trip_id,
    f.VendorID,
    d.tpep_pickup_datetime,
    d.tpep_dropoff_datetime,
    p.passenger_count,
    t.trip_distance,
    r.rate_code_name,
    pick.pickup_latitude,
    pick.pickup_longitude,
    drop.dropoff_latitude,
    drop.dropoff_longitude,
    pay.payment_type_name,
    f.fare_amount,
    f.extra,
    f.mta_tax,
    f.tip_amount,
    f.tolls_amount,
    f.improvement_surcharge,
    f.total_amount
FROM 
    fact AS f
JOIN 
    datetime_dim AS d ON f.datetime_id = d.datetime_id
JOIN 
    passenger_count_dim AS p ON p.passenger_count_id = f.passenger_count_id  
JOIN 
    trip_distance_dim AS t ON t.trip_distance_id = f.trip_distance_id  
JOIN 
    rate_code_dim AS r ON r.rate_code_id = f.rate_code_id  
JOIN 
    pickup_location_dim AS pick ON pick.pickup_location_id = f.pickup_location_id
JOIN 
    dropoff_location_dim AS drop ON drop.dropoff_location_id = f.dropoff_location_id
JOIN 
    payment_type_dim AS pay ON pay.payment_type_id = f.payment_type_id;
=======

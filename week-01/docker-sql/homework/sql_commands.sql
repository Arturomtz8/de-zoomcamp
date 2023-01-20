-- Question 3. Count records

-- How many taxi trips were totally made on January 15?

-- Tip: started and finished on 2019-01-15.

-- Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.


select count(1) from ny_taxi_data where lpep_pickup_datetime::date = '2019-01-15' 
and lpep_dropoff_datetime::date = '2019-01-15';


-- Question 4. Largest trip for each day

-- Which was the day with the largest trip distance 

-- Use the pick up time for your calculations.


select lpep_pickup_datetime from ny_taxi_data where trip_distance 
in (select max(trip_distance) from ny_taxi_data);


-- Question 5. The number of passengers

-- In 2019-01-01 how many trips had 2 and 3 passengers?

select count(1) from ny_taxi_data where lpep_pickup_datetime::date = '2019-01-01' and
passenger_count = 2;

select count(1) from ny_taxi_data where lpep_pickup_datetime::date = '2019-01-01' and
passenger_count = 3;




-- Question 6. Largest tip

-- For the passengers picked up in the Astoria Zone 
-- which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

--  change type of the columns from bigint to text
alter table ny_taxi_backup 
alter column "PULocationID" type text using "PULocationID"::text

alter table ny_taxi_backup 
alter column "DOLocationID" type text using "DOLocationID"::text

-- replace values from DOLocationID AND PULocationID using the Zone 

UPDATE ny_taxi_backup
SET "DOLocationID" = ny_taxi_zones."Zone"
FROM ny_taxi_zones
WHERE ny_taxi_backup."DOLocationID" = ny_taxi_zones."LocationID"::text;

UPDATE ny_taxi_backup
SET "PULocationID" = ny_taxi_zones."Zone"
FROM ny_taxi_zones
WHERE ny_taxi_backup."PULocationID" = ny_taxi_zones."LocationID"::text;

-- select the drop off location id in Astoria zone 

select "DOLocationID"  from ny_taxi_backup
WHERE tip_amount in (select max(tip_amount) from ny_taxi_backup where "PULocationID"='Astoria');


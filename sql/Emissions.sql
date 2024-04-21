create table emission_standard(
stnd text primary key, 
cert_region CHAR(2), 
stnd_description text
);

create table car_details(
underhood_id text,
stnd text REFERENCES emission_standard(stnd),
model text,
disp numeric,
cyl numeric,
trans text,
drive text,
fuel text,
veh_class text,
air_pollution_score numeric,
city_mpg numeric,
city_mpg_alternate numeric,
hwy_mpg numeric,
hwy_mpg_alternate numeric,
cmb_mpg numeric,
cmb_mpg_alternate numeric,
greenhouse_gas_score numeric,
smart_way text,
comb_co2 numeric,
comb_co2_alternate numeric,
primary key (underhood_id, stnd)
);

/* To delete all data from both tables
*/
truncate emission_standard cascade;



-- Create extension and foreign server
CREATE EXTENSION IF NOT exists postgres_fdw;
CREATE SERVER foreign_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'database-3.c5go4e6kame4.us-east-1.rds.amazonaws.com', dbname 'postgres', port '5432');

-- Create user mapping
CREATE USER MAPPING FOR postgres
SERVER foreign_server
OPTIONS (user 'postgres', password 'javamylife');

-- Import foreign schema
IMPORT FOREIGN SCHEMA public
FROM SERVER foreign_server
INTO public;

SELECT *
FROM 
    (SELECT *
     FROM car_details
     INNER JOIN emission_standard ON car_details.stnd = emission_standard.stnd) AS details
INNER JOIN 
    (SELECT *
     FROM electric_vehicles
     INNER JOIN counties ON electric_vehicles.county_id = counties.id) AS population
ON details.cert_region = population.state;

DROP FOREIGN TABLE public.counties, public.electric_vehicles , public.vehicle_primary_uses;

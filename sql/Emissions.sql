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

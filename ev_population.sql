CREATE TABLE electric_vehicles (
    id SERIAL PRIMARY KEY,
    date DATE,
    county_id INT REFERENCES counties(id),
    vehicle_primary_use_id INT REFERENCES vehicle_primary_uses(id),
    bev INT,
    phev INT,
    ev_total INT,
    non_ev_total INT,
    total_vehicles INT,
    percent_ev FLOAT
);

CREATE TABLE counties (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    state VARCHAR(255)
);

CREATE TABLE vehicle_primary_uses (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255)
);

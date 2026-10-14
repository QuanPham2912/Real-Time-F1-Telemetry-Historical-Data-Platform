CREATE TABLE public.dim_session (
    session_key bigint NOT NULL,
    session_id text,
    session_type text,
    session_date timestamp
    with
        time zone,
        race_id text,
        race_name text,
        season integer,
        round integer,
        circuit_id text,
        circuit_name text,
        lat real,
        long real,
        locality text,
        country text,
        CONSTRAINT dim_session_pkey PRIMARY KEY (session_key)
);

CREATE TABLE public.dim_driver (
    driver_key bigint NOT NULL,
    driver_id text,
    driver_tla text,
    given_name text,
    family_name text,
    date_of_birth date,
    nationality text,
    CONSTRAINT dim_driver_pkey PRIMARY KEY (driver_key)
);

CREATE TABLE public.dim_constructor (
    constructor_key bigint NOT NULL,
    constructor_id text,
    name text,
    nationality text,
    started_time text,
    CONSTRAINT dim_constructor_pkey PRIMARY KEY (constructor_key)
);

CREATE TABLE public.dim_engine_supplier (
    engine_manufacturer_key bigint NOT NULL,
    engine_manufacturer text,
    nation text,
    started_time text,
    CONSTRAINT dim_engine_supplier_pkey PRIMARY KEY (engine_manufacturer_key)
);

CREATE TABLE public.fact_result (
    session_key bigint NOT NULL,
    driver_key bigint NOT NULL,
    constructor_key bigint NOT NULL,
    engine_manufacturer_key bigint,
    chassis text,
    position integer,
    grid integer,
    laps integer,
    is_completed_full_laps boolean,
    points real,
    total_race_time_ms double precision,
    gap_to_leader text,
    fastest_lap_rank integer,
    fastest_lap_number integer,
    fastest_lap_time text,
    average_speed real,
    units text,
    status text,
    result_status_description text,
    season integer,
    CONSTRAINT fact_result_pkey PRIMARY KEY (
        session_key,
        driver_key,
        constructor_key
    )
);

CREATE TABLE public.fact_lap (
    session_key bigint,
    driver_key bigint,
    constructor_key bigint,
    permanent_number integer,
    lap_number bigint,
    lap_time real,
    sector_1_time real,
    sector_2_time real,
    sector_3_time real,
    lap_start_time real,
    is_lap_start_time_estimated boolean,
    lap_end_time real,
    is_lap_end_time_estimated boolean,
    stint bigint,
    compound text,
    tyre_life integer,
    fresh_tyre boolean,
    pit_in_time real,
    pit_out_time real,
    track_status text,
    is_accurate boolean,
    season integer
);

CREATE TABLE public.fact_telemetry (
    session_key bigint,
    driver_key bigint,
    permanent_number integer,
    lap_number bigint,
    event_time timestamp
    with
        time zone NOT NULL,
        session_time real,
        lap_time real,
        speed double precision,
        throttle double precision,
        brake boolean,
        rpm integer,
        n_gear integer,
        drs integer,
        distance double precision,
        x double precision,
        y double precision,
        z double precision,
        season integer
);

CREATE TABLE public.fact_weather (
    session_key bigint,
    session_time real,
    air_temp double precision,
    track_temp double precision,
    humidity double precision,
    rain_fall boolean,
    wind_speed double precision,
    wind_direction integer,
    pressure double precision,
    season integer
);

CREATE INDEX ON public.fact_lap (
    session_key,
    driver_key,
    lap_number
);

CREATE INDEX ON public.fact_telemetry (
    session_key,
    driver_key,
    lap_number
);

CREATE INDEX ON public.fact_weather (session_key);
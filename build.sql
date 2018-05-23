drop schema if exists import cascade;
create schema import;

-- drop table if exists import.master_plan;
create table import.master_plan(
    start_time_utc text,
    duration text,
    date text,
    team text,
    spass_type text,
    target text,
    request_name text,
    library_definition text,
    title text,
    description text
);
drop table if exists import.inms;
CREATE TABLE import.inms (
    sclk text,
    uttime text,
    target text,
    time_ca text,
    targ_pos_x text,
    targ_pos_y text,
    targ_pos_z text,
    source text,
    data_reliability text,
    table_set_id text,
    coadd_cnt text,
    osp_fil_1_status text,
    oss_fil_2_status text,
    csp_fil_3_status text,
    css_fil_4_status text,
    seq_table text,
    cyc_num text,
    cyc_table text,
    scan_num text,
    trap_table text,
    sw_table text,
    mass_table text,
    focus_table text,
    da_table text,
    velocity_comp text,
    ipnum text,
    mass_per_charge text,
    os_lens2 text,
    os_lens1 text,
    os_lens4 text,
    os_lens3 text,
    qp_lens2 text,
    qp_lens1 text,
    qp_lens4 text,
    qp_lens3 text,
    qp_bias text,
    ion_defl2 text,
    ion_defl1 text,
    ion_defl4 text,
    ion_defl3 text,
    top_plate text,
    p_energy text,
    alt_t text,
    view_dir_t_x text,
    view_dir_t_y text,
    view_dir_t_z text,
    sc_pos_t_x text,
    sc_pos_t_y text,
    sc_pos_t_z text,
    sc_vel_t_x text,
    sc_vel_t_y text,
    sc_vel_t_z text,
    sc_vel_t_scx text,
    sc_vel_t_scy text,
    sc_vel_t_scz text,
    lst_t text,
    sza_t text,
    ss_long_t text,
    distance_s text,
    view_dir_s_x text,
    view_dir_s_y text,
    view_dir_s_z text,
    sc_pos_s_x text,
    sc_pos_s_y text,
    sc_pos_s_z text,
    sc_vel_s_x text,
    sc_vel_s_y text,
    sc_vel_s_z text,
    lst_s text,
    sza_s text,
    ss_long_s text,
    sc_att_angle_ra text,
    sc_att_angle_dec text,
    sc_att_angle_tw text,
    c1counts text,
    c2counts text
);
drop table if exists import.cda;
create table import.cda(
  event_id text,
  impact_event_time text,
  impact_event_julian_date text,
  qp_amplitude text,
  qi_amplitude text,
  qt_amplitude text,
  qc_amplitude text,
  spacecraft_sun_distance text,
  spacecraft_saturn_distance text,
  spacecraft_x_velocity text,
  spacecraft_y_velocity text,
  spacecraft_z_velocity text,
  counter_number text,
  particle_mass text,
  particle_charge text
);
COPY import.master_plan FROM '/home/jason/project/a_curious_moon_tut/data/master_plan.csv' WITH DELIMITER ',' HEADER CSV;
COPY import.inms FROM '/home/jason/project/a_curious_moon_tut/data/INMS/inms.csv' WITH DELIMITER ',' HEADER CSV;
COPY import.cda FROM '/home/jason/project/a_curious_moon_tut/data/CDA/cda.csv' WITH DELIMITER ',' HEADER CSV;
drop table if exists events cascade;
drop table if exists event_types cascade;
drop table if exists requests cascade;
drop table if exists spass_types cascade;
drop table if exists targets cascade;
drop table if exists teams cascade;

select distinct(library_definition)
as description
into event_types
from import.master_plan;

alter table event_types
add id serial primary key;

select distinct(request_name)
as description
into requests
from import.master_plan;

alter table requests
add id serial primary key;

select distinct(spass_type)
as description
into spass_types
from import.master_plan;

alter table spass_types
add id serial primary key;

select distinct(target)
as description
into targets
from import.master_plan;

alter table targets
add id serial primary key;

select distinct(team)
as description
into teams
from import.master_plan;

alter table teams
add id serial primary key;

create table events(
    id serial primary key,
    time_stamp timestamptz not null,
    title varchar(500),
    description text,
    event_type_id int references event_types(id),
    spass_type_id int references spass_types(id),
    target_id     int references targets(id),
    team_id       int references teams(id),
    request_id    int references requests(id)
);

insert into events(
    time_stamp,
    title,
    description,
    event_type_id,
    target_id,
    team_id,
    request_id,
    spass_type_id
)
select import.master_plan.start_time_utc::timestamptz,
       import.master_plan.title,
       import.master_plan.description,
       event_types.id as event_type_id,
       targets.id     as target_id,
       teams.id       as team_id,
       requests.id    as request_id,
       spass_types.id as spass_type_id
from import.master_plan
left join event_types
    on event_types.description = import.master_plan.library_definition
left join targets
    on targets.description = import.master_plan.target
left join teams
    on teams.description = import.master_plan.team
left join requests
    on requests.description = import.master_plan.request_name
left join spass_types
    on spass_types.description = import.master_plan.spass_type;

drop materialized view if exists enceladus_events;
create materialized view enceladus_events as
select
    events.id,
    events.title,
    events.description,
    events.time_stamp,
    events.time_stamp::date as date,
    event_types.description as event,
    to_tsvector(
        concat(events.description,' ', events.title)
    ) as search
from events
inner join event_types
    on event_types.id = events.event_type_id
where target_id = (select id from targets where description = 'Enceladus')
order by time_stamp;

create index idx_event_search
on enceladus_events using GIN(search);

-- cleat up import.inms by removing headers and empty rows
delete from import.inms
where sclk IS NULL or sclk = 'sclk';

drop materialized view if exists flyby_altitudes;
create materialized view flyby_altitudes as
select
    (sclk::timestamp) as time_stamp,
    date_part('year', (sclk::timestamp)) as year,
    date_part('week', (sclk::timestamp)) as week,
    alt_t::numeric(10,3) as altitude
from import.inms
where target = 'ENCELADUS'
    and alt_t is not null;

drop function if exists low_time(
    numeric,
    double precision,
    double precision
);
create function low_time(
    alt numeric,
    yr double precision,
    wk double precision,
    out timestamp without time zone
) as $$
select
    min(time_stamp) + (( max(time_stamp) - min(time_stamp)) /2) as nadir
    from flyby_altitudes
    where flyby_altitudes.altitude = alt
    and flyby_altitudes.year = yr
    and flyby_altitudes.week = wk
$$ language sql;

-- convenience for redoing
drop table if exists flybys;
with lows_by_week as (
    select year, week,
    min(altitude) as altitude
    from flyby_altitudes
    group by year, week
), nadirs as (
    select low_time(altitude, year, week) as time_stamp,
        altitude
    from lows_by_week
)

-- exec the CTE
select nadirs.*,
    -- set initial vals to null
    null::varchar as name,
    null::timestamptz as start_time,
    null::timestamptz as end_time
-- push to a new table
into flybys
from nadirs;
-- add pk
alter table flybys
add column id serial primary key;
-- using the key, create
-- the name using the new id
-- || cancatenates strings
-- and also coerces to string
update flybys
set name='E-' || id-1;

drop schema if exists cda cascade;
create schema cda;
select
    icda.event_id::integer as id,
    icda.impact_event_time::timestamp as time_stamp,
    icda.impact_event_time::date as impact_date,
    case icda.counter_number
        when '** ' then null
        else counter_number::integer
    end as counter,
    icda.counter_number::integer,
    icda.spacecraft_sun_distance::numeric(6,4) as sun_distance_au,
    icda.spacecraft_saturn_distance::numeric(8,2) as saturn_distance_rads,
    icda.spacecraft_x_velocity::numeric(6,2) as x_velocity,
    icda.spacecraft_y_velocity::numeric(6,2) as y_velocity,
    icda.spacecraft_z_velocity::numeric(6,2) as z_velocity,
    icda.particle_charge::numeric(4,1),
    icda.particle_mass::numeric(4,1)
into cda.impacts
from import.cda as icda
order by icda.impact_event_time::timestamptz;

alter table cda.impacts
add id serial primary key;

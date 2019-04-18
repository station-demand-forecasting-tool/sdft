-- enhanced from:
-- https://gis.stackexchange.com/questions/88196/how-can-i-transform-polylines-into-points-every-n-metres-in-postgis
-- creates a table of virtual nodes for roadlinks
-- node pid created as negative integer
-- requires sequence called vnodes_pid_seq starting at 10000000
create table openroads.vnodesneg_roadlinks as
-- select long edges (1.5km)
with line as
    (select
        id as edge_id, the_geom as geom
    from openroads.roadlinks
			where cost_len > 1500
			),
-- create measure every 1km
linemeasure as
    (select
        edge_id, st_addmeasure(line.geom, 0, st_length(line.geom)) as linem,
        generate_series(0, st_length(line.geom)::int, 1000) as i
    from line),
geometries as (
    select
        i, linem, edge_id,
        (st_dump(st_geometryn(st_locatealong(linem, i), 1))).geom as geom
        -- note the .geom construct because st_dump returns a geom component and
        -- an array of integers (path).
    from linemeasure),
points as (
    select
        nextval('vnodes_pid_seq'::regclass) as pid, i, linem, edge_id,
        st_setsrid(st_makepoint(st_x(geom), st_y(geom)), 27700) as geom
from geometries),
fractions as (
select -pid as pid, edge_id, geom as the_geom, st_linelocatepoint(linem, geom)
as fraction from points order by pid asc)
-- only want nodes that are not existing source or target nodes
select * from fractions where fraction > 0 and fraction < 1

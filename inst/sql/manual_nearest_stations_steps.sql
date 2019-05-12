-- replicate the choice set generation process used in the sql functions
-- need to run model as far as generating model centroidnodes materialized view.

-- find the id of origin node
select pid from job001.centroidnodes a where a.reference = 'TR182AB'

-- find which service area to use (has 10 or more stations)
with tmp as (select n_geom from job001.centroidnodes where reference = 'TR130BX')
select
case
when
(select count ( * ) from job001.stations a, tmp b where st_within(b.n_geom, a.service_area_1km) ) >= 10 then
'service_area_1km'
when ( select count ( * ) from job001.stations a, tmp b where st_within(b.n_geom, a.service_area_5km) ) >= 10 then
'service_area_5km'
when ( select count ( * ) from job001.stations a, tmp b where st_within(b.n_geom, a.service_area_10km) ) >= 10 then
'service_area_10km'
when ( select count ( * ) from job001.stations a, tmp b where st_within(b.n_geom, a.service_area_20km) ) >= 10 then
'service_area_20km'
when ( select count ( * ) from job001.stations a, tmp b where st_within(b.n_geom, a.service_area_30km) ) >= 10 then
'service_area_30km'
when ( select count ( * ) from job001.stations a, tmp b where st_within(b.n_geom, a.service_area_40km) ) >= 10 then
'service_area_40km'
when ( select count ( * ) from job001.stations a, tmp b where st_within(b.n_geom, a.service_area_60km) ) >= 10 then
'service_area_60km'
when ( select count ( * ) from job001.stations a, tmp b where st_within(b.n_geom, a.service_area_80km) ) >= 10 then
'service_area_80km'  else 'service_area_105km' end


-- create temporary table with details of the stations within the selected service area
create temp table n10 as (
  with tmp as (select reference, n_geom from job001.centroidnodes where reference = 'TR182AB'),
  tmp2 as (
    select 'TR182AB' as postcode, d.name,
    d.crscode, d.service_area_30km, d.location_geom, e.pid from job001.stations d left join
    job001.centroidnodes e on d.crscode = e.reference
  )
  select a.postcode, a.crscode, a.name, a.location_geom, a.pid, b.n_geom from tmp2 a
  left join tmp b on a.postcode = b.reference
  where st_within((n_geom), service_area_30km)
)

-- lookup the distances.
select postcode, name, crscode, r.agg_cost as distance from n10 as d
/* lateral runs the pgr function for each row of d */
  /* left join lateral ensures nulls are returned - these need bigger bbox */
  left join lateral openroads.bbox_pgr_withpointscost(
    'select id, source, target, cost_len as cost, the_geom
          from openroads.roadlinks',
    $node$select pid*-1 as pid, edge_id, frac::double precision as
    fraction from job001.centroidnodes where (type = 'station' or pid =
                                                -20406540) and pid <0$node$,
    -20406540,
    d.pid,
    false,
    d.n_geom,
    d.location_geom,
    expand_min := 1000,
    expand_pc := 0.5) r on true
order by distance asc

-- drop temp table
drop table n10

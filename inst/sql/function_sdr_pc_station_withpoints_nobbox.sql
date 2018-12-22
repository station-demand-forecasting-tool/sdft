create or replace function openroads.sdr_pc_station_withpoints_nobbox("pc" text, "crs" text)
returns table("distance" float8) as $body$
  declare
sa character varying;
node_sql character varying;
origin_geom geometry;
origin_node bigint;
station_geom geometry;
station_node bigint;

-- find the first service area where origin intersects 10 or more stations.
-- don't do more work then we need.
begin

select geom from data.pc_pop_2011 a where a.postcode = pc into origin_geom;
select location_geom from model.stations a where a.crscode = crs into station_geom;
select pid from model.centroidnodes a where a.reference = pc into origin_node;
select pid from model.centroidnodes a where a.reference = crs into station_node;

-- do node sql separately
node_sql := format ( 'select pid*-1 as pid, edge_id, frac::double precision as fraction from model.centroidnodes where (pid = %s or pid = %s) and pid <0', station_node, origin_node);
return query execute format ( '
select d.agg_cost from pgr_withpointscost(
  $edges$select id, source, target, cost_len as cost, the_geom
  from openroads.roadlinks$edges$,
  $1,
  $2,
  $3,
  false) as d
') using
node_sql,
origin_node,
station_node
return;
end
$body$
language plpgsql volatile

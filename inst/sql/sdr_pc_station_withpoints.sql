-- gets distance between a named postcode and station with points
-- intended to be used to widen the bbox for stations where distance
-- returned null when looking for the nearest 10 stations
create or replace function sdr_pc_station_withpoints (
  pc text, crs text, expand_min int8 default 0, expand_pc float4 default 0.5 )
returns table ( distance float8 )
as $body$
  declare
sa character varying;
node_sql character varying;
origin_geom geometry;
origin_node bigint;
station_geom geometry;
station_node bigint;


begin

select geom from data.pc_pop_2011 a where a.postcode = pc into origin_geom;
select location_geom from data.stations a where a.crscode = crs into station_geom;
select pid from model.centroidnodes a where a.reference = pc into origin_node;
select pid from model.centroidnodes a where a.reference = crs into station_node;

-- do node sql separately
node_sql := format ( 'select pid*-1 as pid, edge_id, frac::double precision as fraction from model.centroidnodes where (pid = %s or pid = %s) and pid <0', station_node, origin_node);
return query execute format ( '
select d.agg_cost from bbox_pgr_withpointscost(
  $edges$select id, source, target, cost_len as cost, the_geom
  from openroads.roadlinks$edges$,
  $1,
  $2,
  $3,
  false,
  $4,
  $5,
  expand_min := $6,
  expand_pc := $7) as d
') using
		node_sql,
		origin_node,
		station_node,
		origin_geom,
		station_geom,
		expand_min,
		expand_pc;
		return;
end
$body$ language plpgsql;

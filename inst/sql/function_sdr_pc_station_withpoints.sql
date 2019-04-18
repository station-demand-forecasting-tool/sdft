-- updated 03/2019 to handle model schema as parameter
-- gets distance between a named postcode and station with points
-- intended to be used to widen the bbox for stations where distance
-- returned null when looking for the nearest 10 stations

create or replace function "openroads"."sdr_pc_station_withpoints"("schema"
text, "pc" text, "crs" text, "expand_min" int8=0, "expand_pc" float4=0.5)
  returns table("distance" float8) as $body$
declare
sa character varying;
node_sql character varying;
origin_geom geometry;
origin_node bigint;
station_geom geometry;
station_node bigint;


begin

select geom from data.pc_pop_2011 a where a.postcode = pc into origin_geom;

execute
  format (' select location_geom from %1$s.stations a where a.crscode = $1 ', schema)
  using
   crs into station_geom;

execute
  format (' select pid from %1$s.centroidnodes a where a.reference = $1 ', schema)
  using
   pc into origin_node;

execute format (' select pid from %1$s.centroidnodes a where a.reference = $1 ', schema)
  using
    crs into station_node;

-- do node sql separately
node_sql := format ( 'select pid*-1 as pid, edge_id, frac::double precision as
fraction from %1$s.centroidnodes where (pid = %2$s or pid = %3$s) and pid <0',
schema, station_node, origin_node);
return query execute format ( '
			select d.agg_cost from openroads.bbox_pgr_withpointscost(
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
$BODY$
  LANGUAGE plpgsql VOLATILE

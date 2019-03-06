
-- amended 3/2019 to take schema as parameter
-- postcode version of function to find nearest stations
-- postcode (text) is sent to function rather than node and a geom
create or replace function "openroads"."sdr_pc_nearest_stationswithpoints"("schema" text, "pc" text, "expand_min" int8=0, "expand_pc" float4=0.5)
  returns table("postcode" text, "station" text, "crscode" text, "distance" float8) as $body$
declare
sa character varying;
node_sql character varying;
origin_geom geometry;
origin_node bigint;

-- find the first service area where origin intersects 10 or more stations.
-- don't do more work then we need.
begin

select geom from data.pc_pop_2011 a where a.postcode = pc into origin_geom;

execute format ('select pid from %1$s.centroidnodes a where a.reference = $1', schema ) using pc into origin_node;

execute format ('
select
	case
		when
			( select count ( * ) from %1$s.stations where st_within($1, service_area_1km) ) >= 10 then
				$$service_area_1km$$
				when ( select count ( * ) from %1$s.stations where st_within($1, service_area_5km) ) >= 10 then
				$$service_area_5km$$
				when ( select count ( * ) from %1$s.stations where st_within($1, service_area_10km) ) >= 10 then
				$$service_area_10km$$
				when ( select count ( * ) from %1$s.stations where st_within($1, service_area_20km) ) >= 10 then
				$$service_area_20km$$
				when ( select count ( * ) from %1$s.stations where st_within($1, service_area_30km) ) >= 10 then
				$$service_area_30km$$
				when ( select count ( * ) from %1$s.stations where st_within($1, service_area_40km) ) >= 10 then
				$$service_area_40km$$
				when ( select count ( * ) from %1$s.stations where st_within($1, service_area_30km) ) >= 10 then
				$$service_area_60km$$
				when ( select count ( * ) from %1$s.stations where st_within($1, service_area_40km) ) >= 10 then
				$$service_area_80km$$ else $$service_area_105km$$ end', schema) using origin_geom into sa;


-- define virtual node sql, restrict to station vnodes (type = station) or the origin pid and only ever pids < 0)
-- have to separate this out to inject the origin node.
node_sql := format ( 'select pid*-1 as pid, edge_id, frac::double precision as fraction from %1$s.centroidnodes where (type = $type$station$type$ or pid = %2$s) and pid <0', schema, origin_node );
return query execute format ( '
			/* cte table with intersected stations for the selected service area with station node pid joined */
			with tmp as (select $1 as postcode, d.name, d.crscode, d.%2$s, d.location_geom, e.pid from %1$s.stations d
			left join %1$s.centroidnodes e on d.crscode = e.reference
			where st_within($2, %2$s))
			select postcode, name, crscode, r.agg_cost as distance from tmp as d
			/* lateral runs the pgr function for each row of d */
			/* left join lateral ensures nulls are returned - these need bigger bbox */
			left join lateral openroads.bbox_pgr_withpointscost(
			$edges$select id, source, target, cost_len as cost, the_geom
			from openroads.roadlinks$edges$,
			$3,
			$4,
			d.pid,
			false,
			$2,
			d.location_geom,
			expand_min := $5,
			expand_pc := $6) r on true
			order by distance asc', schema, sa) using
		pc,
		origin_geom,
		node_sql,
		origin_node,
		expand_min,
		expand_pc;
		return;
end
$body$
  language plpgsql

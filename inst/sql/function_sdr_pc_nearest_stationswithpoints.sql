-- postcode version of function to find nearest stations
-- postcode (text) is sent to function rather than node and a geom
create or replace function sdr_pc_nearest_stationswithpoints (
  pc text, expand_min int8 default 0, expand_pc float4 default 0.5 )
returns table ( name text, crscode text, distance float8 )
as $body$
  declare
sa character varying;
node_sql character varying;
origin_geom geometry;
origin_node bigint;

-- find the first service area where origin intersects 10 or more stations.
-- don't do more work then we need.
begin

select geom from data.pc_pop_2011 where postcode = pc into origin_geom;
select pid from model.centroidnodes where reference = pc into origin_node;

select
case
when
( select count ( * ) from data.stations where origin_geom && service_area_1km ) >= 10 then
'service_area_1km'
when ( select count ( * ) from data.stations where origin_geom && service_area_5km ) >= 10 then
'service_area_5km'
when ( select count ( * ) from data.stations where origin_geom && service_area_10km ) >= 10 then
'service_area_10km'
when ( select count ( * ) from data.stations where origin_geom && service_area_20km ) >= 10 then
'service_area_20km'
when ( select count ( * ) from data.stations where origin_geom && service_area_30km ) >= 10 then
'service_area_30km'
when ( select count ( * ) from data.stations where origin_geom && service_area_40km ) >= 10 then
'service_area_40km'
when ( select count ( * ) from data.stations where origin_geom && service_area_30km ) >= 10 then
'service_area_60km'
when ( select count ( * ) from data.stations where origin_geom && service_area_40km ) >= 10 then
'service_area_80km' else 'service_area_105km'
end into sa;
-- define virtual node sql, restrict to station vnodes (pid <= -30000000) or the origin pid and only ever pids < 0)
-- have to separate this out to inject the origin node.
node_sql := format ( 'select pid*-1 as pid, edge_id, frac::double precision as fraction from model.centroidnodes where (pid <= -30000000 or pid = %s) and pid <0', origin_node );
return query execute format ( '
			/* cte table with intersected stations for the selected service area with station node pid joined */
			with tmp as (select d.name, d.crscode, d.%1$s, d.location_geom, e.pid from data.stations d
			left join model.centroidnodes e on d.crscode = e.reference
			where $1 && %1$s)
			select name, crscode, r.agg_cost as distance from tmp as d
			/* lateral runs the pgr function for each row of d */
			/* left join lateral ensures nulls are returned - these need bigger bbox */
			left join lateral bbox_pgr_withpointscost(
			$edges$select id, source, target, cost_len as cost, the_geom
			from openroads.roadlinks$edges$,
			$2,
			$3,
			d.pid,
			false,
			$1,
			d.location_geom,
			expand_min := $4,
			expand_pc := $5) r ON true
			order by distance asc limit 10', sa) using origin_geom,
			node_sql,
			origin_node,
			expand_min,
			expand_pc;
			return;
			end
			$body$ language plpgsql;

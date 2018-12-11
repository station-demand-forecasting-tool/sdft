-- function to obtain the nearest 10 railway stations to given origin
-- currently uses bbox_pgr_dijkstracost
-- to be amended to use bbox_pgr_withpointscost
-- also need additional parameters - e.g. tolerance for finding nearest edge

-- requires that service areas have been created in the station table
-- this version is withoutpoints.

create or replace function sdr_nearest_stations(origin geometry, expand_min int8 default 0, expand_pc float4 default 0.5) returns
table(name text, crscode text, distance float8)
as $body$
DECLARE
	sa character VARYING;
-- find the first service area where origin intersects 10 or more stations.
-- don't do more work then we need.
begin
	Select case
		when (select count (*) from data.stations where origin && service_area_1km) >= 10 then 'service_area_1km'
		when (select count (*) from data.stations where origin && service_area_5km) >= 10 then 'service_area_5km'
		when (select count (*) from data.stations where origin && service_area_10km) >= 10 then 'service_area_10km'
		when (select count (*) from data.stations where origin && service_area_20km) >= 10 then 'service_area_20km'
		when (select count (*) from data.stations where origin && service_area_30km) >= 10 then 'service_area_30km'
		when (select count (*) from data.stations where origin && service_area_40km) >= 10 then 'service_area_40km'
		when (select count (*) from data.stations where origin && service_area_30km) >= 10 then 'service_area_60km'
		when (select count (*) from data.stations where origin && service_area_40km) >= 10 then 'service_area_80km'
		else 'service_area_105km' end
	into sa;
	return query execute format ('select name, crscode, r.agg_cost as distance from data.stations as d,
	lateral bbox_pgr_dijkstracost(
		$sql$select id,
		source,
		target,
		cost_len as cost,
		the_geom
		from openroads.roadlinks$sql$,
		$1,
		d.location_geom,
		false,
		tol_dist := 1000,
		expand_min := $2,
	expand_pc := $3) r
	where $1 && %I
	order by distance asc limit 10', sa)
	using origin, expand_min, expand_pc;
	return;
	END
	$body$ language plpgsql;

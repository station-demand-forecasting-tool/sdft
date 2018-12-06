-- function to obtain the nearest 10 railway stations to given origin
-- currently uses bbox_pgr_dijkstracost
-- to be amended to use bbox_pgr_withpointscost
-- also need additional parameters

create or replace function sdr_nearest_stations(origin geometry, expand_pc float4 default 0.5) returns
table(name text, crscode text, distance numeric)
as $$
DECLARE
	sa character VARYING;
begin
	Select case
		when (select count (*) from data.stations where origin && service_area_1km) >= 10 then 'service_area_1km'
		when (select count (*) from data.stations where origin && service_area_5km) >= 10 then 'service_area_5km'
		when (select count (*) from data.stations where origin && service_area_10km) >= 10 then 'service_area_10km'
		when (select count (*) from data.stations where origin && service_area_20km) >= 10 then 'service_area_20km'
		when (select count (*) from data.stations where origin && service_area_30km) >= 10 then 'service_area_30km'
		when (select count (*) from data.stations where origin && service_area_40km) >= 10 then 'service_area_40km'
		else 'service_area_67km' end
	into sa;
	return query execute format ('select name, crscode, round(r.agg_cost::numeric, 2) as distance from data.stations as d,
	lateral bbox_pgr_dijkstracost(
		$sql$select id,
		source,
		target,
		cost_time as cost,
		the_geom
		from openroads.roadlinks$sql$,
		$1,
		d.location_geom,
		false,
		tol_dist := 1000,
	expand_percent := $2) r
	where $1 && %I
	order by distance asc limit 10', sa)
	using origin, expand_pc;
	return;
	END
	$$ language plpgsql;

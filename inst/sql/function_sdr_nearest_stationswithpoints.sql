-- sdr_nearest_stationswithpoints

-- work in progress, this is a working function call
-- not how only use the relevant part of the virtual nodes table
-- this considerablt imporves performance.

-- needs to be incoporated into sdr_nearest_stationswithpoints function (or additional with and without points)
-- quite slow. Perhaps better just to insert the vnodes that are needed for this query?
with tmp as (select d.*, e.pid from data.stations d
left join model.centroidnodes e on d.crscode = e.reference)
Select name, crscode, round(r.agg_cost::numeric, 2) as distance from tmp as d,
lateral bbox_pgr_withpointscost(
		$edges$select id, source, target, cost_len as cost, the_geom
		from openroads.roadlinks$edges$,
		$nodes$select pid*-1 as pid, edge_id, frac::double precision as fraction from model.centroidnodes where pid <= -30000000 or pid = -20009863$nodes$,
		-20009863,
		d.pid,
		false,
		st_setsrid(st_point(248217,75089), 27700),
		d.location_geom,
	tol_dist := 1000,
	expand_percent := 0.4) r
	where st_point(248217,75089) && service_area_30km
	order by distance asc limit 10

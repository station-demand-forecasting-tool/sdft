-- example queries


-- using bbox_pgr_dijkstra enhanced function
-- function also finds nearest real node? on nearest edge
select
	r.*
from
	bbox_pgr_dijkstra (
		'
		select id,
		source,
		target,
		cost_time as cost,
		the_geom
		from openroads.roadlinks',
		st_transform ( st_setsrid ( st_point ( - 3.1901573, 50.799663 ), 4326 ), 27700 ),
		st_transform ( st_setsrid ( st_point ( - 2.9348337, 50.724873 ), 4326 ), 27700 ),
		false,
		tol_dist := 100,
	expand_percent := 0.3
	) as r


-- using bbox_pgr_withpoints enhanced function
-- start and end nodes would come from a generated node table for
-- each postcode centroid and each station which would have real and virtual nodes.
-- examples here is using virtual nodes from roadlinks which are
-- actually used for station service areas.
select
	*
from
	bbox_pgr_withpoints (
		'
		select id, source, target, cost_len as cost, the_geom
		from openroads.roadlinks',
		'select pid*-1 as pid, edge_id, fraction from openroads.vnodesneg_roadlinks',
		- 10028781,
		- 10014591,
		false,
		start_geom := st_transform ( st_setsrid ( st_point ( - 3.96936, 52.04480 ), 4326 ), 27700 ),
		end_geom := st_transform ( st_setsrid ( st_point ( - 5.07385, 56.23231 ), 4326 ), 27700 ),
	tol_dist := 100,
	expand_percent := 0.2)

-- example pgr_withpoints
-- start is a real node, destination is a virtual node.
-- virtual nodes pids must be positive (thus *-1)
-- we would select the negative pids (<0) from the combined node table
select
	*
from
	pgr_withpoints (
		'
		select id, source, target, cost_len as cost
		from openroads.roadlinks',
		'select pid*-1 as pid, edge_id, fraction from openroads.vnodesneg_roadlinks where pid < 0',
		1228827,
	- 10028781,
	false)

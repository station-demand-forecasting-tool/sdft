-- expanded version of pgr_dijkstracost to include bounding box and pgr_pointtoedgenode
create
	or replace function openroads.bbox_pgr_dijkstracost (
		sql text,
		start_pt geometry,
		end_pt geometry,
		directed boolean,
		tol_dist float8 default 100,
		expand_min int8 default 0,
		expand_pc float4 default 0.5
	) returns table ( agg_cost float8 ) as $body$ declare
	var_source_id bigint;
var_target_id bigint;
var_new_sql text;
var_expand_geom geometry;
begin
		execute'create temp view edge as ' || sql || '; ';
	var_source_id := pgr_pointtoedgenode ( 'edge', start_pt, tol_dist );
	var_target_id := pgr_pointtoedgenode ( 'edge', end_pt, tol_dist );
	drop view edge;
	var_new_sql := sql;
-- create a bbox that is expanded x percent
-- around the bbox of start and end
	var_expand_geom := st_expand ( st_makeline ( start_pt, end_pt ), greatest(expand_min, st_distance ( start_pt, end_pt ) * expand_pc ));
-- only include edges that overlap the expand bounding box
	var_new_sql := 'select * from (' || sql || ') as e
	where e.the_geom &&  ' || quote_literal( var_expand_geom :: text ) || '::geometry';
	return query select
	d.agg_cost
	from
		pgr_dijkstracost ( var_new_sql, var_source_id, var_target_id, directed ) as d;

end;
$body$ language plpgsql

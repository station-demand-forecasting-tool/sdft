-- bbox_pgr_withpointscost - expands pgr_withpoints to handle bounding box
create or replace function openroads.bbox_pgr_withpointscost(
    sql text, node_sql text, start_vn bigint, end_vn bigint,
    directed boolean,
		start_geom geometry, end_geom geometry,
		expand_min int8 default 0,
    expand_pc float4 default 0.5)
    returns table ("agg_cost" float8) as
$$
  declare
    var_new_sql text; var_expand_geom geometry;
  begin
			-- create a bbox that is expanded x percent
      -- around the bbox of start and end
      var_expand_geom := st_expand(st_makeline(start_geom, end_geom),
      greatest(expand_min, st_distance(start_geom, end_geom) * expand_pc ));
      -- only include edges that overlap the expand bounding box
      var_new_sql := 'select * from (' || sql || ') as e
                where e.the_geom &&  '
                || quote_literal(var_expand_geom::text) || '::geometry';
      return query select d.agg_cost
         from pgr_withpointscost(var_new_sql, node_sql, start_vn,
            end_vn, directed) as d;
  end;
$$ language 'plpgsql';

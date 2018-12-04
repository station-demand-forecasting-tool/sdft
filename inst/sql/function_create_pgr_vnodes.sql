-- Adapted from code block 11.2 from "pgROUTING - a practical guide".
-- Amended to return the geom of the virtual nodes (needed for enhanced bbox functions),
-- or otherwise the geom of a real node if fraction <0.01 (edge start) or >0.99 (edge end)
-- Returns virtual node pids and fraction, or if virtual node is at the same location as a source
-- or target real node (i.e either end of an edge) the source or target node pid
-- is returned (otherwise routing would not work).
create or replace function
    vnodes(network_sql text,
    points geometry[], tolerance float default 0.01)
returns table (id bigint, pid bigint, edge_id bigint,
    fraction float, closest_node bigint, n_geom geometry) as
$$
-- network sql must contain edge_id, geom, source, target
declare var_sql text;
begin
  var_sql := 'with p as (
  select id::bigint as id, f.geom
  from unnest($1) with ordinality as f(geom, id)
    )
 select p.id, case when e.fraction < 0.01 then e.source
         when e.fraction > 0.99 then e.target else -p.id end::bigint as pid,
         e.edge_id::bigint, e.fraction,
         case when e.fraction <= 0.5 then e.source
         else e.target end::bigint as closest_node,
				 case when e.fraction < 0.01 then st_startpoint(e.geom)
				 when e.fraction > 0.99 then st_endpoint(e.geom)
         else e.vn_geom end as n_geom
from p,
    lateral (
    select w.id as edge_id, w.source, w.target,
      st_linelocatepoint(w.geom, p.geom) as fraction,
			st_lineinterpolatepoint(w.geom,st_linelocatepoint(w.geom, p.geom)) as vn_geom,
			w.geom
     from (' || network_sql || ') as w
        where st_dwithin(p.geom, w.geom, $2)
     order by st_distance(p.geom, w.geom) limit 1 ) as e';
 return query execute var_sql using points, tolerance;
end;
$$ language 'plpgsql';

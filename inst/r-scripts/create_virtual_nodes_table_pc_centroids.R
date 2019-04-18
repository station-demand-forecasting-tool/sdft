# create virtual nodes table for use in pgRouting functions
# using postcode centroids and railway stations.

# need to check  if any nulls are returned?
# depends on the search tolerance. Currently set at 1km, can probably
# exclude any postcode centroids not within 1km of an edge?
# ok, so just use lateral not left join lateral so nulls are dropped.


query <- paste0(
  " alter table data.pc_pop_2011 add column id serial unique;
  "
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbGetQuery(con, query)


query <- paste0(
  "create table openroads.centroidnodes as
  with tmp1 as (
  select d.id, d.postcode as reference, 'postcode' as type, f.pid, f.edge_id, f.fraction::double precision as frac, f.closest_node as closest_real_node, f.n_geom
  from data.pc_pop_2011 as d,
  lateral openroads.create_pgr_vnodes(
  $$select id, source, target, the_geom as geom from openroads.roadlinks$$,
  array[d.geom], 1000) f
  ), tmp2 as (
  select d.id, d.crscode as reference, 'station' as type, f.pid, f.edge_id, f.fraction::double precision as frac, f.closest_node as closest_real_node, f.n_geom
  from data.stations as d,
  lateral openroads.create_pgr_vnodes(
  $$select id, source, target, the_geom as geom from openroads.roadlinks$$,
  array[d.location_geom], 1000) f
  ) select reference, case when pid = -1 then (id+20000000)*pid else pid end as pid, type, edge_id, frac, closest_real_node, n_geom from tmp1
  union all
  select reference, case when pid=-1 then (id+30000000)*pid else pid end as pid, type, edge_id, frac, closest_real_node, n_geom from tmp2
  "
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbGetQuery(con, query)

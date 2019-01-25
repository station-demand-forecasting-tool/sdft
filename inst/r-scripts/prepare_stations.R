# This script creates a series of distance-based service areas for each railway station
# It expects the stations table to be in a schema called 'data'.
# This will take a considerable time to complete (24 hours perhaps).
# Service areas will be used when the nearest stations to each postcode centroid need
# to be generated.

# An alternative, instead of creating the service area polygons would be to just store the reachable
# nodes for each distance in an array for each station. Then a text search would be used rather than a spatial
# search to find the nearest stations. This search likely to be much slower though?

library(progress)

# create stations dataframe - populate with crscodes and location coordinates
query <- paste0("select crscode, longitude || ',' || latitude as location from data.stations;")
stations <- dbGetQuery(con, query)

total_stations <- nrow(stations) # set number of records

# service areas required in metres
service_areas_metres <- c(1000,5000,10000,20000,30000,40000,60000,80000,105000)


# begin the service area loop i
for (i in service_areas_metres) {
  # create sa column in table
  query <-
    paste0(
      "alter table data.stations add column if not exists service_area_",
      i / 1000,
      "km geometry(Polygon,27700);"
    )
  dbGetQuery(con, query)

  pb <-
    progress_bar$new(total = total_stations, format = "(:spin) [:bar] :percent")


  ## Note the pid provided in the virtual node sql to pgr_withpointsdd must be negative
  ## for the virtual nodes to be included when searching for nodes within driving distance.
  ## Also note: in this case virtual nodes cannot then be used as source nodes
  ## Source node is taken as the nearest real node on the nearest edge to the station.
  ## This node is found using pgr_pointtoedgenode, with a maximum 1000km tolerance to nearest edge.
  ## A buffer of the same distance is created first to limit size of network considered by pgr_withpointsdd
  ## This improves performance. By definition, road distance will be less than or (theoretically) equal to the buffer
  ## So the buffer will always contain entire road distance area

  # Begin the stations loop j
  for (j in 1:total_stations) {
    pb$tick()   # update progress bar

    query <- paste0(
      "with tmp as
      (
      select dd.node, coalesce(a.the_geom, b.the_geom) as the_geom
      from
      pgr_withpointsdd($sql$
      select id, source, target, cost_len as cost
      from openroads.roadlinks where the_geom && st_buffer(st_transform(st_setsrid(st_point(",
      stations$location[j] ,
      "), 4326), 27700),",
      i ,
      ")$sql$,
      $sql2$select pid, edge_id, fraction from openroads.vnodesneg_roadlinks$sql2$,
      pgr_pointtoedgenode($str1$openroads.roadlinks$str1$, st_transform(st_setsrid(st_point(",
      stations$location[j] ,
      "), 4326), 27700), 1000),",
      i ,
      ", false)
      as dd
      left join openroads.roadnodes as a on dd.node = a.id
      left join openroads.vnodesneg_roadlinks as b on dd.node = -b.pid
      order by node
      )
      update data.stations set service_area_",
      i / 1000 ,
      "km = sa.geom  FROM (
      SELECT 1 as id, ST_ConcaveHull(ST_Collect(the_geom), 0.9) as geom from tmp) as sa
      WHERE stations.crscode = '",
      stations$crscode[j] ,
      "';"
      )
    query <- gsub(
      pattern = '\\s',
      replacement = " ",
      x = query
    )
    dbGetQuery(con, query)
  }
  pb$terminate()
}

## Will need to check for null values for each service area. For some reason a few were null when tested
## This appears to be because ST_ConcaveHull returns null using 0.9, even though there are points.
## This could be a bug.
## Also as at version appears to be a bug when using two decimal places in the target %
## A collection is returned not a polygon.
## For now repeat any nulls with target % set to 1. Seem to resolve it?

## Note: Change above to a function to avoid this code repetition?

# begin the service area loop i
for (i in service_areas_metres) {
  query <-
    paste0(
      "select crscode, longitude || ',' || latitude as location from data.stations where service_area_",
      i / 1000,
      "km is null;"
    )
  stations_null <- dbGetQuery(con, query)

  total_null <- nrow(stations_null) # set number of records

  if (total_null > 0) {
    # Begin the stations loop j
    for (j in 1:total_null) {
      query <- paste0(
        "with tmp as
        (
        select dd.node, coalesce(a.the_geom, b.the_geom) as the_geom
        from
        pgr_withpointsdd($sql$
        select id, source, target, cost_len as cost
        from openroads.roadlinks where the_geom && st_buffer(st_transform(st_setsrid(st_point(",
        stations_null$location[j] ,
        "), 4326), 27700),",
        i ,
        ")$sql$,
        $sql2$select pid, edge_id, fraction from openroads.vnodesneg_roadlinks$sql2$,
        pgr_pointtoedgenode($str1$openroads.roadlinks$str1$, st_transform(st_setsrid(st_point(",
        stations_null$location[j] ,
        "), 4326), 27700), 1000),",
        i ,
        ", false)
        as dd
        left join openroads.roadnodes as a on dd.node = a.id
        left join openroads.vnodesneg_roadlinks as b on dd.node = -b.pid
        order by node
        )
        update data.stations set service_area_",
        i / 1000 ,
        "km = sa.geom  FROM (
        SELECT 1 as id, ST_ConcaveHull(ST_Collect(the_geom), 1) as geom from tmp) as sa
        WHERE stations.crscode = '",
        stations_null$crscode[j] ,
        "';"
        )
      query <- gsub(pattern = '\\s',
                    replacement = " ",
                    x = query)
      dbGetQuery(con, query)
    }
  }
}


# service areas required in time - for abstraction
service_areas_minutes <- c(60)

# for time set the buffer to 105000. The fastest road speed
# in roadlinks is 65mph (105 kmph) so thereotical maximum distance
# that can be acheived in one hour is 105km

buffer <- 105000


# begin the service area loop i
for (i in service_areas_minutes) {
  # create sa column in table
  query <-
    paste0(
      "alter table data.stations add column if not exists service_area_",
      i,
      "mins geometry(Polygon,27700);"
    )
  dbGetQuery(con, query)

  pb <-
    progress_bar$new(total = total_stations, format = "(:spin) [:bar] :percent")


  ## Note the pid provided in the virtual node sql to pgr_withpointsdd must be negative
  ## for the virtual nodes to be included when searching for nodes within driving distance.
  ## Also note: in this case virtual nodes cannot then be used as source nodes
  ## Source node is taken as the nearest real node on the nearest edge to the station.
  ## This node is found using pgr_pointtoedgenode, with a maximum 1000km tolerance to nearest edge.
  ## A buffer of the same distance is created first to limit size of network considered by pgr_withpointsdd
  ## This improves performance. By definition, road distance will be less than or (theoretically) equal to the buffer
  ## So the buffer will always contain entire road distance area

  # Begin the stations loop j
  for (j in 1:total_stations) {
    pb$tick()   # update progress bar

    query <- paste0(
      "with tmp as
      (
      select dd.node, coalesce(a.the_geom, b.the_geom) as the_geom
      from
      pgr_withpointsdd($sql$
      select id, source, target, cost_time as cost
      from openroads.roadlinks where the_geom && st_buffer(st_transform(st_setsrid(st_point(",
      stations$location[j] ,
      "), 4326), 27700),",
      buffer ,
      ")$sql$,
      $sql2$select pid, edge_id, fraction from openroads.vnodesneg_roadlinks$sql2$,
      pgr_pointtoedgenode($str1$openroads.roadlinks$str1$, st_transform(st_setsrid(st_point(",
      stations$location[j] ,
      "), 4326), 27700), 1000),",
      i ,
      ", false)
      as dd
      left join openroads.roadnodes as a on dd.node = a.id
      left join openroads.vnodesneg_roadlinks as b on dd.node = -b.pid
      order by node
      )
      update data.stations set service_area_",
      i ,
      "mins = sa.geom  FROM (
      SELECT 1 as id, ST_ConcaveHull(ST_Collect(the_geom), 0.9) as geom from tmp) as sa
      WHERE stations.crscode = '",
      stations$crscode[j] ,
      "';"
      )
    query <- gsub(
      pattern = '\\s',
      replacement = " ",
      x = query
    )
    dbGetQuery(con, query)
  }
  pb$terminate()
}

## Will need to check for null values for each service area. For some reason a few were null when tested
## This appears to be because ST_ConcaveHull returns null using 0.9, even though there are points.
## This could be a bug.
## Also as at version appears to be a bug when using two decimal places in the target %
## A collection is returned not a polygon.
## For now repeat any nulls with target % set to 1. Seem to resolve it?

## Note: Change above to a function to avoid this code repetition?

# begin the service area loop i
for (i in service_areas_minutes) {
  query <-
    paste0(
      "select crscode, longitude || ',' || latitude as location from data.stations where service_area_",
      i ,
      "mins is null;"
    )
  stations_null <- dbGetQuery(con, query)

  total_null <- nrow(stations_null) # set number of records

  if (total_null > 0) {
    # Begin the stations loop j
    for (j in 1:total_null) {
      query <- paste0(
        "with tmp as
        (
        select dd.node, coalesce(a.the_geom, b.the_geom) as the_geom
        from
        pgr_withpointsdd($sql$
        select id, source, target, cost_time as cost
        from openroads.roadlinks where the_geom && st_buffer(st_transform(st_setsrid(st_point(",
        stations_null$location[j] ,
        "), 4326), 27700),",
        buffer ,
        ")$sql$,
        $sql2$select pid, edge_id, fraction from openroads.vnodesneg_roadlinks$sql2$,
        pgr_pointtoedgenode($str1$openroads.roadlinks$str1$, st_transform(st_setsrid(st_point(",
        stations_null$location[j] ,
        "), 4326), 27700), 1000),",
        i ,
        ", false)
        as dd
        left join openroads.roadnodes as a on dd.node = a.id
        left join openroads.vnodesneg_roadlinks as b on dd.node = -b.pid
        order by node
        )
        update data.stations set service_area_",
        i ,
        "minutes = sa.geom  FROM (
        SELECT 1 as id, ST_ConcaveHull(ST_Collect(the_geom), 1) as geom from tmp) as sa
        WHERE stations.crscode = '",
        stations_null$crscode[j] ,
        "';"
        )
      query <- gsub(pattern = '\\s',
                    replacement = " ",
                    x = query)
      dbGetQuery(con, query)
    }
  }
}




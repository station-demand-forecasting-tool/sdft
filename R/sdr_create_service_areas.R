#' Creates a distance or time based network service area around stations
#'
#' Creates a distance or time based network service area around stations in
#' the provided data frame using the pgRouting function \code{pgr_withpointsdd}. The
#' service area geometry is written to the specified database table in the
#' specified database schema.
#'
#' @param schema A text string for the database schema name.
#' @param df A dataframe with a column called "location" which has easting and
#' northing of the station location.
#' @param sa A vector of integer values for the required service areas. Should
#' be in metres when cost is distance and minutes when cost is time.
#' @param table A text string for the database table name.
#' @param cost A text string, either "len" for a distance-based service area or
#' "time" for a time-based service area. Default is "len".
#' @param target The target percent of area of convex hull that ST_ConvexHull
#' will try to approach before giving up or exiting. Default is 0.9.
#' @export
sdr_create_service_areas <- function(schema, df, sa, table, cost = "len", target = 0.9) {

# set number of records
total_stations <- nrow(df)

# begin the service area loop i
for (i in sa) {

  if (cost == "len") {
    column_name <- paste0("service_area_", i / 1000, "km")
    # for distance set buffer to same as distance - not possible for road
    # distance to go beyond straight line distance
    buffer <- i
  } else if (cost == "time") {
    column_name <- paste0("service_area_", i, "mins")
    # for time set the buffer to 105000. The fastest road speed
    # in roadlinks is 65mph (105 kmph) so thereotical maximum distance
    # that can be acheived in one hour is 105km
    buffer <- 105000
  } else {
    stop("cost type does not exist")
  }

  # create sa column in table

  query <-
    paste0(
      "alter table ", paste0(schema, '.', table), " add column if not exists ",
      column_name,
      " geometry(Polygon,27700);"
    )
  getQuery(con, query)

  ## Note the pid provided in the virtual node sql to pgr_withpointsdd must be negative
  ## for the virtual nodes to be included when searching for nodes within driving distance.
  ## Also note: in this case virtual nodes cannot then be used as source nodes
  ## Source node is taken as the nearest real node on the nearest edge to the station.
  ## This node is found using pgr_pointtoedgenode, with a maximum 1000km tolerance to nearest edge.

  # Begin the stations loop j
  for (j in 1:total_stations) {
    query <- paste0(
      "with tmp as
      (
      select dd.node, coalesce(a.the_geom, b.the_geom) as the_geom
      from
      pgr_withpointsdd($sql$
      select id, source, target, cost_", cost, " as cost
      from openroads.roadlinks where the_geom && st_buffer(st_setsrid(st_point(",
      df$location[j] ,
      "), 27700),",
      buffer ,
      ")$sql$,
      $sql2$select pid, edge_id, fraction from openroads.vnodesneg_roadlinks$sql2$,
      pgr_pointtoedgenode($str1$openroads.roadlinks$str1$, st_setsrid(st_point(",
      df$location[j] ,
      "), 27700), 1000),",
      i ,
      ", false)
      as dd
      left join openroads.roadnodes as a on dd.node = a.id
      left join openroads.vnodesneg_roadlinks as b on dd.node = -b.pid
      order by node
      )
      update ", paste0(schema, '.', table), " set ",
      column_name ,
      " = sa.geom  FROM (
      SELECT 1 as id, ST_ConcaveHull(ST_Collect(the_geom), ", target, " ) as geom from tmp) as sa
      WHERE crscode = '",
      df$crscode[j] ,
      "';"
    )
    getQuery(con, query)

    # check for null service area returned  - a potential problem with ST_ConcaveHull with
    # target < 1
    # If null, repeat with target set to 1

    query <-
      paste0(
        "select location from " , paste0(schema, '.', table), " where ", column_name, " is null;"
      )

    stations_null <- getQuery(con, query)

    total_null <- nrow(stations_null) # set number of records

    if (total_null > 0) {

      query <- paste0(
        "with tmp as
      (
      select dd.node, coalesce(a.the_geom, b.the_geom) as the_geom
      from
      pgr_withpointsdd($sql$
      select id, source, target, cost_", cost, " as cost
      from openroads.roadlinks where the_geom && st_buffer(st_setsrid(st_point(",
        df$location[j] ,
        "), 27700),",
        buffer ,
        ")$sql$,
      $sql2$select pid, edge_id, fraction from openroads.vnodesneg_roadlinks$sql2$,
      pgr_pointtoedgenode($str1$openroads.roadlinks$str1$, st_setsrid(st_point(",
        df$location[j] ,
        "), 27700), 1000),",
        i ,
        ", false)
      as dd
      left join openroads.roadnodes as a on dd.node = a.id
      left join openroads.vnodesneg_roadlinks as b on dd.node = -b.pid
      order by node
      )
      update ", paste0(schema, '.', table), " set ",
        column_name ,
        " = sa.geom  FROM (
      SELECT 1 as id, ST_ConcaveHull(ST_Collect(the_geom), 1) as geom from tmp) as sa
      WHERE crscode = '",
        df$crscode[j] ,
        "';"
      )
      getQuery(con, query)

    }

  }
}

# end function
}


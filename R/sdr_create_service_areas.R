#' Creates a distance or time based network service area around stations
#'
#' Creates a distance or time based network service area around stations in
#' the provided data frame using the pgRouting function \code{pgr_withpointsdd}.
#' The service area geometry is written to the specified database \code{table}
#' in the specified database \code{schema}. The supplied \code{identifier} links
#' the stations in the data frame and the database table. Uses parallel processing.
#'
#' @param con An RPostgres database connection object.
#' @param out_path Path to the output folder for logging.
#' @param schema Character, the database schema name.
#' @param df A data frame with a column named "location"
#' which has the easting and northing of the station location.
#' @param identifier Character, uniquely identifies a station in both the
#' provided data frame \emph{and} in the target \code{table}.
#' @param sa Numeric vector of integer values for the required service areas.
#' Should be in metres when cost is distance and minutes when cost is time.
#' @param table Character, the target database table name.
#' @param columns Logical, whether service area columns need to be created in the
#' target database \code{table}. Default is FALSE.
#' @param cost Character, either "len" for a distance-based service area or
#' "time" for a time-based service area. Default is "len".
#' @param target Numeric, the target percent of area of convex hull that
#' ST_ConvexHull will try to approach before giving up or exiting. Default is 0.9.
#' @export
sdr_create_service_areas <-
  function(con,
           out_path,
           schema,
           df,
           identifier,
           sa,
           table,
           columns = TRUE,
           cost = "len",
           target = 0.9) {
    j <- NULL

    # set number of records
    total_stations <- nrow(df)

    # begin the service area loop i
    for (i in sa)
    {
      if (cost == "len") {
        column_name <- paste0("service_area_", i / 1000, "km")
        # for distance set buffer to same as distance - not possible for road
        # distance to go beyond straight line distance
        buffer <- i
      } else if (cost == "time") {
        column_name <- paste0("service_area_", i, "mins")
        # for time set the buffer to 105000. The fastest road speed
        # in roadlinks is 65 mph (105 kmph) so thereotical maximum distance
        # that can be acheived in one hour is 105 km
        buffer <- 105000
      } else {
        stop("Cost type does not exist")
      }


      if (isTRUE(columns)) {
        # create sa column in table
        query <-
          paste0(
            "alter table ",
            paste0(schema, '.', table),
            " add column if not exists ",
            column_name,
            " geometry(Polygon,27700);"
          )
        sdr_dbExecute(con, query)
      }

      # Note that the pid provided in the virtual node sql to pgr_withpointsdd
      # must be negative for the virtual nodes to be included when searching
      # for nodes within driving distance. We want these included as they
      # provide extra nodes on roads with few intersections (e.g. motorways).
      # Also note: in this use-case virtual nodes cannot then be used as source
      # nodes. The source node is therefore taken as the nearest real node on the
      # nearest edge to the station. This node is found using pgr_pointtoedgenode,
      # with a maximum 1000 km tolerance to nearest edge.

      # ST_ConcaveHull can return Points, Linestrings, or Polygons so
      # necessary to buffer any service area returned as a point or linestring
      # thus creating a polygon that can be written to the database table.

      # Begin the stations loop j
      #for (j in 1:total_stations) {
      foreach::foreach(
        j = 1:total_stations,
        .noexport = "con",
        .packages = c("DBI", "RPostgres", "dplyr")
      ) %dopar%
        {
          # futile.logger can't write to sdr.log within a foreach
          # there's probably a solution but a workaround is to use cat().
          # write to a separate file for each worker - compile after %dopar%
          if (threshold == "DEBUG" |
              threshold == "INFO") {
            cat(
              paste0(
                "INFO [",
                format(Sys.time()),
                "] Creating ",
                column_name,
                " for ",
                df[j, paste0(identifier)],
                "\n"
              ),
              file = file.path(out_path, paste0(j, ".log"), fsep = .Platform$file.sep),
              append = TRUE
            )
          }
          query <- paste0(
            "with tmp as
      (
      select dd.node, coalesce(a.the_geom, b.the_geom) as the_geom
      from
      pgr_withpointsdd($sql$
      select id, source, target, cost_",
            cost,
            " as cost
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
      ),
      tmp2 as (select ST_ConcaveHull(ST_Collect(ST_Force2D(the_geom)), ",
            target,
            ") as geom from tmp)
      update ",
            paste0(schema, '.', table),
            " set ",
            column_name ,
            " = sa.geom  FROM (
      SELECT 1 as id,
      case
        when ST_GeometryType(geom) != 'ST_Polygon' then st_buffer(geom, 10)
        else geom
        end
        as geom
        from tmp2) as sa
      WHERE ",
            identifier,
            " = '",
            df[j, paste0(identifier)] ,
            "';"
          )
          sdr_dbExecute(con, query)

          # check for null service area returned  - a potential problem with
          # ST_ConcaveHull when target < 1
          # If null, repeat with target set to 1
          query <-
            paste0(
              "select ",
              identifier,
              " from " ,
              paste0(schema, '.', table),
              " where ",
              column_name,
              " is null and ",
              identifier,
              " = '",
              df[j, paste0(identifier)],
              "'"
            )
          stations_null <- sdr_dbGetQuery(con, query)

          total_null <- nrow(stations_null) # set number of records
          if (total_null > 0) {
            if (threshold == "DEBUG" |
                threshold == "INFO") {
              cat(
                paste0(
                  "INFO [",
                  format(Sys.time()),
                  "] Null returned for ",
                  column_name,
                  " for ",
                  df[j, paste0(identifier)],
                  ": re-running with target = 1",
                  "\n"
                ),
                file = file.path(out_path, paste0(j, ".log"), fsep = .Platform$file.sep),
                append = TRUE
              )
            }
            query <- paste0(
              "with tmp as
      (
      select dd.node, coalesce(a.the_geom, b.the_geom) as the_geom
      from
      pgr_withpointsdd($sql$
      select id, source, target, cost_",
              cost,
              " as cost
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
      ),
      tmp2 as (select ST_ConcaveHull(ST_Collect(ST_Force2D(the_geom)), 1) as geom from tmp)
      update ",
              paste0(schema, '.', table),
              " set ",
              column_name ,
              " = sa.geom  FROM (
      SELECT 1 as id,
      case
        when ST_GeometryType(geom) != 'ST_Polygon' then st_buffer(geom, 10)
        else geom
        end
        as geom
        from tmp2) as sa
      WHERE ",
              identifier,
              " = '",
              df[j, paste0(identifier)] ,
              "';"
            )
            sdr_dbExecute(con, query)
          }
        } # end %dopar%
    } # end for i in sa

    # collect log files into sa.log
    for (i in 1:total_stations) {
      file.append(
        file.path(out_path, "sa.log", fsep = .Platform$file.sep),
        file.path(out_path, paste0(i, ".log"), fsep = .Platform$file.sep)
      )
      file.remove(file.path(out_path, paste0(i, ".log"), fsep = .Platform$file.sep))
    }
    # append sa.log to sdr.log
    cat(
      sort(readr::read_lines(
        file = file.path(out_path, "sa.log", fsep = .Platform$file.sep)
      )),
      file = file.path(out_path, "sdr.log", fsep = .Platform$file.sep),
      append = TRUE,
      fill = TRUE
    )
    file.remove(file.path(out_path, "sa.log", fsep = .Platform$file.sep))


  } # end function

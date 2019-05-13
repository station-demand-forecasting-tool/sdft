#' Generates a choice set of ten nearest stations for postcodes
#'
#' For a given station or stations identifies the postcodes within 60 minutes of
#' the station or stations and then generates a choice set of the nearest 10
#' stations to each of those postcodes. The function is also able to generate
#' before and after choicesets for abstraction analysis.
#'
#' If \code{existing} is set to FALSE then the crscode(s) in \code{crs} must be
#' for proposed stations. The function obtains the 60 minute service area geometry
#' from the schema.proposed_stations table. If there is more than one crscode in
#' \code{crs} then st_union will be applied to the individual service area
#' geometries to create a single merged 60 minute service area (this is used when
#' the concurrent mode has been selected for the model run). The function uses
#' parallel processing via the foreach package and requires the clusters to be
#' configured prior to calling the function.
#'
#' If \code{existing} is set to TRUE then the crscode(s) in \code{crs} must be
#' existing stations (normally a single station). The function will obtain the
#' 60 minute service area geometry from the data.stations table.
#'
#' If \code{abs_crs} is passed to the function then it should be a single crscode
#' of an existing station. This is used to control whether a before or after set
#' of postcode choicesets is required. To obtain a before choiceset for
#' abstraction analysis pass the existing station crscode to the function using
#' \code{crs}, set \code{existing} to TRUE and do not pass \code{abs_crs}. To
#' obtain an after choiceset for abstraction analysis pass the proposed station(s)
#' in \code{crs}, set \code{existing} to FALSE and pass the crscode of the
#' existing station for which abstraction analysis is being carried out to
#' \code{abs_crs}.
#'
#' The function submits db queries which rely on several bespoke pgRouting wrapper
#' functions that must be located in the openroads schema: \code{create_pgr_vnodes},
#' \code{sdr_crs_pc_nearest_stationswithpoints}, \code{sdr_pc_station_withpoints},
#' \code{sdr_pc_station_withpoints_nobbox}, and \code{bbox_pgr_withpointscost}.
#'
#' Two materialized views are created for use by these functions. The first is
#' schema.centroidnodes which is a union of virtual nodes for the proposed
#' stations (which are created in the query) and virtual nodes for existing
#' stations. The second is schema.stations which is a union of stations from
#' data.stations and schema.proposed_stations containing the distance-based
#' service areas used in identifying the nearest 10 stations to each postcode.
#'
#' For maximum flexibility to also use this function for abstraction analysis
#' without code repetition, it should be noted that the union queries are
#' used even when \code{existing} is set to TRUE and the crscode in \code{crs} is
#' an existing station. In this case there will be no match with any stations in
#' schema.proposed_stations and that part of the union query will simply return
#' null.
#'
#' @param schema Character, the database schema name.
#' @param crs Character vector of the crscode(s) of the station(s)
#' for which a set of postcode choicesets is required.
#' @param existing Logical. Indicates whether the station crscodes contained in
#' \code{crs} are for existing (TRUE) or proposed (FALSE) stations.
#' Default is FALSE.
#' @param abs_crs Character, a single crs code of an existing station for which
#' an after set of postcode choicesets is required. Optional.
#' @return Returns a data frame containing the postcode choicesets with stations
#' ranked by distance from the postcode centroid.
#' @importFrom foreach %dopar%
#' @export
sdr_generate_choicesets <-
  function(schema,
           crs,
           existing = FALSE ,
           abs_crs = NULL) {
    # Set which station the set of postcode choicesets is required for
    # This will be the content of crs, or abs_crs if it is specified.
    if (is.null(abs_crs)) {
      pc_crs = crs
    } else {
      pc_crs = abs_crs
    }
    futile.logger::flog.info(paste0(
      "Set of postcode choice sets is required for: ",
      paste0(pc_crs, collapse = ", ")
    ))

    # Set the table to be used to get the service area geometry.
    # If existing is TRUE always use schema.proposed_stations. Otherwise
    # it depends on whether abs_crs is specified or not.
    if (isTRUE(existing)) {
      pc_table = "data.stations"
    } else if (is.null(abs_crs)) {
      pc_table = paste0(schema, ".proposed_stations")
    } else {
      pc_table = "data.stations"
    }
    futile.logger::flog.info(paste0("Using service area geometry from: ", pc_table))
    futile.logger::flog.info(paste0(
      "Getting postcodes within 60 minute service area of: ",
      paste0(pc_crs, collapse = ", ")
    ))

    # Get postcodes within the applicable 60 minute service area
    query <- paste0(
      "
      with sa as (
      select st_union(geom) as geom from (
      select service_area_60mins
      as geom from ",
      pc_table,
      " where crscode in (",
      paste ("'", pc_crs, "'", sep = "", collapse = ", ") ,
      ")
      ) as foo
      )
      select
      postcode
      from
      data.pc_pop_2011 a, sa where st_within(a.geom, sa.geom)
      "
    )
    postcodes <- sdr_dbGetQuery(con, query)

    # Need to create virtual nodes for proposed station(s).
    # Create view of nodes table union with the proposed station(s)
    # including all existing stations but only the postcode nodes
    # that are needed, i.e. within the sa (so query above is used again in
    # the where statement).
    # The first CTE (tmp) is getting virtual nodes for the proposed station(s);
    # then select the station and relevant postcode nodes from
    # openroads.centroidnodes; then union all select the virtual node information
    # from tmp.

    futile.logger::flog.info("Creating virtual nodes table")

    query <- paste0(
      "create materialized view ",
      schema,
      ".centroidnodes as
      with tmp as (
      select
      d.id,
      d.crscode as reference,
      'station' as type,
      f.pid,
      f.edge_id,
      f.fraction :: double precision as frac,
      f.closest_node as closest_real_node,
      f.n_geom
      from ",
      schema,
      ".proposed_stations as d,
      lateral openroads.create_pgr_vnodes ( $$ select id, source, target, the_geom as geom from openroads.roadlinks$$, array [ d.location_geom ], 1000 ) f
      where crscode in (",
      paste("'", crs, "'", sep = "", collapse = ",")
      ,
      ")
      ) select
      *
      from
      openroads.centroidnodes where
      type = 'station' or
      reference in (
      with sa as (
      select st_union(geom) as geom from (
      select service_area_60mins
      as geom from ",
      pc_table,
      " where crscode in (",
      paste ("'", pc_crs, "'", sep = "", collapse = ", ") ,
      ")
      ) as foo
      )
      select
      postcode
      from
      data.pc_pop_2011 a, sa where st_within(a.geom, sa.geom)
      )
      UNION ALL
      select
      reference,
      case
      when pid = -1 then
      ( id + 40000000 ) * pid else pid
      end as pid,
      type,
      edge_id,
      frac,
      closest_real_node,
      n_geom
      from
      tmp
      "
    )
    sdr_dbExecute(con, query)

    # create index

    query <- paste0("create index idx_centroidnodes_pid
    on ", schema, ".centroidnodes(pid)")
    sdr_dbExecute(con, query)

    query <- paste0("create index idx_centroidnodes_reference
    on ", schema, ".centroidnodes(reference)")
    sdr_dbExecute(con, query)

    # create view of existing and proposed station(s)
    futile.logger::flog.info(paste0(
      "Creating stations view for existing stations and: ",
      paste0(crs, collapse = ", ")
    ))

    query <- paste0(
      "
      create materialized view ",
      schema,
      ".stations as
      select crscode,
      name,
      location_geom,
      service_area_1km,
      service_area_5km,
      service_area_10km,
      service_area_20km,
      service_area_30km,
      service_area_40km,
      service_area_60km,
      service_area_80km,
      service_area_105km
      from data.stations
      UNION ALL
      select crscode,
      name,
      location_geom,
      service_area_1km,
      service_area_5km,
      service_area_10km,
      service_area_20km,
      service_area_30km,
      service_area_40km,
      service_area_60km,
      service_area_80km,
      service_area_105km
      from ",
      schema,
      ".proposed_stations
      where crscode in (",
      paste("'", crs, "'", sep = "", collapse = ", ")
      ,
      ")
      "
    )
    sdr_dbExecute(con, query)

    # create spatial indexes for the service areas

    sa_names <- c("service_area_1km",
                  "service_area_5km",
                  "service_area_10km",
                  "service_area_20km",
                  "service_area_30km",
                  "service_area_40km",
                  "service_area_60km",
                  "service_area_80km",
                  "service_area_105km")

    for (sa_name in sa_names) {
    query <- paste0("create index idx_stations_", sa_name,
    " on ", schema, ".stations using gist(", sa_name, ")")
    sdr_dbExecute(con, query)
    }

    # generate choicesets using parallel processing
    futile.logger::flog.info(
      paste0(
        "Starting parallel processing to generate the choicesets for ",
        nrow(postcodes),
        " postcodes"
      )
    )
    df <-
      foreach::foreach(
        i = postcodes$postcode,
        .noexport = "con",
        .packages = c("DBI", "RPostgres", "dplyr"),
        .combine = 'rbind'
      ) %dopar%
      {
        query <- paste0(
          "
          select postcode, crscode, distance from openroads.sdr_crs_pc_nearest_stationswithpoints('",
          schema,
          "', '",
          i,
          "', '",
          paste(crs, sep = "", collapse = ", "),
          "', 1000, 0.5)
          "
        )
        nearestx <- sdr_dbGetQuery(con, query)
        # sdr_crs_pc_nearest_stationswithpoints() will return null if no
        # station in crs is present in the nearest 10 (or more) stations
        # this avoids unnecessary distance lookups. Therefore, need to check
        # rows returned are > 0 before
        if (nrow(nearestx) > 0) {
          # check for distance NAs. If present make individual function call for
          # each affected postcode:station pair with a larger bounding box.
          for (j in which(is.na(nearestx$distance))) {
            query <- paste0(
              "
              select distance from openroads.sdr_pc_station_withpoints('"
              ,
              schema,
              "', '",
              nearestx$postcode[j],
              "', '",
              nearestx$crscode[j],
              "', 25000, 1)
              "
            )
            d <- sdr_dbGetQuery(con, query)
            # check if a distance is returned
            if (nrow(d) > 0) {
              nearestx$distance[j] <- d$distance
            } else {
              # if still no result then query without any bbox
              query <- paste0(
                "
                select distance from openroads.sdr_pc_station_withpoints_nobbox('"
                ,
                schema,
                "', '",
                nearestx$postcode[j],
                "', '",
                nearestx$crscode[j],
                "')
                "
              )
              d <- sdr_dbGetQuery(con, query)
              # just in case still no result trap and use -9999
              if (nrow(d) > 0) {
                nearestx$distance[j] <- d$distance
              } else {
                nearestx$distance[j] <- -9999
              }
            }
          } # end for distance NA
          choiceset <-
            nearestx %>% dplyr::mutate("distance_rank" = dplyr::row_number(distance)) %>%
            dplyr::filter(distance_rank <= 10) %>%
            dplyr::arrange(distance_rank)
          } # end if nearestx not empty
        } # end foreach postcode parallel processing

    # drop materialized views

    query <- paste0("
                drop materialized view " ,
      schema,
      ".centroidnodes
                ")
    sdr_dbExecute(con, query)

    query <- paste0("
                drop materialized view " ,
                    schema, ".stations
                ")
    sdr_dbExecute(con, query)

    # remove rows (i.e. the choiceset) for any postcodes where none of the crscodes
    # are in the choiceset
    futile.logger::flog.info(paste0(
      "Set of choicesets generated for: ",
      paste0(pc_crs, collapse = ", ")
    ))
    futile.logger::flog.info(paste0("Rows: ", nrow(df)))
    futile.logger::flog.info(paste0(
      "Removing all rows for any postcode where none of: ",
      paste0(pc_crs, collapse = ", ")
    ))
    df <- df %>%
      dplyr::group_by(postcode) %>%
      # note: dplyr filter(any(...)) evaluates at the group_by() level
      dplyr::filter(any(crscode %in% pc_crs))

    futile.logger::flog.info(paste0("Rows: ", nrow(df)))
    futile.logger::flog.info(paste0("Unique postcodes: ", length(unique(df$postcode))))

    # remove rows (i.e. the choiceset) for any postcode where one or more
    # postcode:station distances could not be obtained - distance value will be -9999
    # first generate warning
    if (length(which(df$distance == -9999)) > 0) {
      pc_missing_d <- df %>% dplyr::filter(any(distance == -9999))
      msg <-
        paste0(
          length(unique(pc_missing_d$postcode)),
          " postcode(s) removed from
          choicesets due to missing distance: ",
          paste0(
            pc_missing_d$postcode[pc_missing_d$distance == -9999],
            ":",
            pc_missing_d$crscode[pc_missing_d$distance == -9999],
            collapse = ", "
          )
        )
      futile.logger::flog.warn(msg)
      # now filter to remove postcodes that contain a -9999 distance
      df <- df %>%
        dplyr::filter(!any(distance == -9999))

      futile.logger::flog.info(paste0("Rows: ", nrow(df)))
      futile.logger::flog.info(paste0("Unique postcodes: ", length(unique(df$postcode))))
    }

    # get some choiceset stats
    avg_choiceset_size <- df %>%
      dplyr::summarise(number = n()) %>%
      dplyr::summarise(mean(number))
    min_choiceset_size <- df %>%
      dplyr::summarise(number = n()) %>%
      dplyr::summarise(min(number))
    max_choiceset_size <- df %>%
      dplyr::summarise(number = n()) %>%
      dplyr::summarise(max(number))

    # info or warnings re choiceset dimensions
    if (avg_choiceset_size < 10 |
        max_choiceset_size > 10 | min_choiceset_size < 10) {
      futile.logger::flog.warn(
        paste0(
          "Choiceset dimensions outside of expected values. Average size: ",
          avg_choiceset_size,
          "; maximum size: ",
          max_choiceset_size,
          "; minimum size: ",
          min_choiceset_size
        )
      )
    } else {
      # log the choiceset stats
      futile.logger::flog.info(paste0("average choiceset size: ",
                                      avg_choiceset_size))
      futile.logger::flog.info(paste0("min choiceset size: ",
                                      min_choiceset_size))
      futile.logger::flog.info(paste0("max choiceset size: ",
                                      max_choiceset_size))
    }

    # remove the postcode grouping from df
    df <- dplyr::ungroup(df)
    return(df)
    #end function
  }

#' Generates a choice set of ten nearest stations for postcodes
#'
#' For a given station or stations identifies the postcodes within 60 minutes of
#' the station or stations and then generates a choice set of the nearest 10 stations to each of those
#' postcodes. The function is also able to generate before and after choicesets for abstraction analysis.
#'
#' If existing is set to \code{FALSE} then the crscodes in \code{crs} must be proposed
#' stations. The function will obtain the 60 minute service area geometry from the
#' model.proposed_stations table. If there is more than one crscode in \code{crs} then
#' st_union will be applied to the individual service area geometries to create a
#' single merged 60 minute service area (this is used when the concurrent mode has
#' been selected for the model run).
#'
#' If existing is set to \code{TRUE} then the crscodes in \code{crs} must be existing
#' stations (normally a single station). The function will obtain the 60 minute service
#' area geometry from the data.stations table.
#'
#' If \code{abs_crs} is passed to the function then it should be a single crscode
#' of an existing station. This is used to control whether a before or after set of
#' postcode choicesets is required. To obtain a before choiceset for abstraction analysis
#' pass the existing station crscode to the function using \code{crs}, set \code{existing}
#' to \code{TRUE} and do not pass \code{abs_crs}. To obtain an after choiceset for
#' abstraction analysis pass the proposed station(s) in \code{crs}, set \code{existing}
#' to \code{FALSE} and pass the crscode of the existing station for which abstraction analysis
#' is being carried out to \code{abs_crs}.
#'
#' The function uses parallel processing via the foreach package and requires
#' the clusters to be configured prior to calling the function.
#'
#' The function submits db queries which rely on several pgRouting wrapper functions that must be
#' located in the openroads schema: \code{create_pgr_vnodes},
#' \code{sdr_crs_pc_nearest_stationswithpoints}, \code{sdr_pc_station_withpoints}, and
#' \code{sdr_pc_station_withpoints_nobbox}.
#'
#' Two views are created for use by these functions. The first is model.centroidnodes
#' which is a union of virtual nodes for the proposed stations (which are created in the query)
#' and virtual nodes for existing stations. The second is model.stations which is a union of
#' stations from data.stations and model.proposed_stations containing the distance-based
#' service areas used in identfying the nearest 10 stations to each postcode.
#'
#' For maximum flexibility to also use this function for abstraction analysis
#' without code repetition, it should be noted that the union queries are
#' used even when \code{existing} is set to \code{TRUE} and the crscode in \code{crs} is
#' an existing station. In this case there will be no match with the stations in
#' model.proposed_stations and that part of the union query will simply return null.
#'
#' @param crs A character vector of the crscode(s) of the station(s)
#' for which a set of postcode choicesets is required.
#' @param existing Logical. Indicates whether the station crscodes contained in
#' \code{crs} are for existing (\code{TRUE}) or proposed (\code{FALSE}) stations.
#' Default is \code{FALSE}.
#' @param abs_crs Character. A single crs code of an existing station for which an
#' after set of postcode choicesets is required. Optional.
#' @return Returns a data frame containing the postcode choicesets with stations ranked
#' by distance.
#' @importFrom foreach %dopar%
#' @export
sdr_generate_choicesets_parallel <- function(crs, existing = FALSE , abs_crs = NULL ) {

  # Set the station crs codes that will be used for the 60 minute service area
  # used to identify relevant postcodes. This will be the content of crs unless
  # abs_crs is specified.
  if (is.null(abs_crs)) {
    pc_crs = crs
  } else {
    pc_crs = abs_crs
  }

  # Set the table to be used to get the service area geometry.
  # If existing is TRUE always use model.proposed_stations. Otherwise
  # it depends on whether abs_crs is specified or not.
  if (existing == TRUE) {
    pc_table = "data.stations"
  } else if (is.null(abs_crs)) {
    pc_table = "model.proposed_stations"
  } else {
    pc_table = "data.stations"
  }

  # Get postcodes within proposed station(s) 60 minute service area
  query <- paste0(
    "
    with sa as (
    select st_union(geom) as geom from (
    select service_area_60mins
    as geom from ", pc_table, " where crscode in (",
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
    postcodes <- getQuery(con, query)


  # Need to create virtual nodes for new stations.
  # Create view of nodes table union with the new stations
  # including all existing stations but only the postcode nodes
  # that are needed, i.e. within the sa (so query above is used again in
  # the where statement).
  # First CTE (tmp) is getting virtual nodes for the proposed station(s);
  # then select the station and relevant postcode nodes from openroads.centroidnodes;
  # then union all select the virtual node information from tmp.

  query <- paste0(
    "create or replace view model.centroidnodes as
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
    from
    model.proposed_stations as d,
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
    as geom from ", pc_table, " where crscode in (",
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
    when pid =- 1 then
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
  getQuery(con, query)

  query <- paste0(
    "
    create or replace view model.stations as
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
    from model.proposed_stations
    where crscode in (",
    paste("'", crs, "'", sep = "", collapse = ", ")
    ,
    ")
    "
    )
  getQuery(con, query)

  # generate choicesets using parallel processing

  df <- foreach::foreach(i=postcodes$postcode, .noexport="con", .packages=c("DBI", "RPostgreSQL", "dplyr"), .combine = 'rbind') %dopar%
  {

    query <- paste0(
      "
      select postcode, crscode, distance from openroads.sdr_crs_pc_nearest_stationswithpoints('",
      i,
      "', '",
      paste (crs, sep = "", collapse = ", "),
      "', 1000, 0.5)
      "
      )
    nearestx <- getQuery(con, query)

    if (nrow(nearestx) > 0) {

      # check for nulls. If present
      # make individual function call for each specific postcode:station pair
      # with larger bounding box.
      for (j in which(is.na(nearestx$distance))) {
        query <- paste0(
          "
          select distance from openroads.sdr_pc_station_withpoints('"
          , nearestx$postcode[j],
          "', '", nearestx$crscode[j],
          "', 25000, 1)
          "
          )
        d <- getQuery(con, query)
        # check if a distance is returned
        if (nrow(d) > 0) {
          nearestx$distance[j] <- d$distance
        } else {
          # if still no result then query without any bbox
          query <- paste0(
            "
          select distance from openroads.sdr_pc_station_withpoints_nobbox('"
            , nearestx$postcode[j],
            "', '", nearestx$crscode[j],
            "')
          "
          )
          d <- getQuery(con, query)
          # just in case still no result trap and use -9999
          # how to deal with this??
          nearestx$distance[j] <- ifelse(nrow(d) > 0, d$distance, -9999)
        }

      } # end for each null station

      choiceset <- nearestx %>% dplyr::mutate("distance_rank" = dplyr::row_number(distance)) %>%
        dplyr::filter(distance_rank <= 10) %>%
        dplyr::arrange(distance_rank)


    }  # end if nearestx not empty

  } # end foreach postcode parallel processing


  return(df)

  #end function
}

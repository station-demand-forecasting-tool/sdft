
sdr_generate_choicesets_parallel <- function(crs) {

  # get postcodes within proposed station(s) 60 minute service area
  query <- paste0(
    "
    with sa as (
    select st_union(geom) as geom from (
    select service_area_60mins
    as geom from model.proposed_stations where crscode in (",
    paste ("'", crs, "'", sep = "", collapse = ", ") ,
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


  # Need to create virtual nodes for new stations
  # create view of nodes table union with the new stations
  # including all existing stations but only the postcode nodes
  # that are needed, i.e. within the sa (so query above is used again in
  # the where statement)
  # first cte tmp is getting virtual nodes for the proposed station(s)
  # then select the station and relevant postcode nodes from openroads.centroidnodes
  # then union all select the virtual node information from the cte tmp

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
    as geom from model.proposed_stations where crscode in (",
    paste ("'", crs, "'", sep = "", collapse = ", ") ,
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

  df <- foreach(i=postcodes$postcode, .noexport="con", .packages=c("DBI", "RPostgreSQL", "dplyr"), .combine = 'rbind') %dopar%
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

      choiceset <- nearestx %>% mutate("distance_rank" = row_number(distance)) %>%
        filter(distance_rank <= 10) %>%
        arrange(distance_rank)


    }  # end if nearestx not empty

  } # end foreach postcode parallel processing


  return(df)

  #end function
}

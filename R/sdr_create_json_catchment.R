#' Creates a GeoJSON probabilistic catchment for a station
#'
#' Creates a GeoJSON probabilistic catchment for a station. Includes a spatial
#' query to check that the postcode is within the 60 minute service area of the
#' station (important for "concurrent" mode).dev
#'
#' @param crs crscode of the station
#' @param tablesuffix The suffix of the probability table - either crscode
#' (isolation) or 'concurrent' (concurrent) is expected.
#' @export
sdr_create_json_catchment <- function(crs, tablesuffix) {
  query <- paste0(
    "update model.proposed_stations set catchment = (select row_to_json(fc)
    from (
    select
    'FeatureCollection' as \"type\",
    array_to_json(array_agg(f)) as \"features\"
    from (
    select
    'Feature' as \"type\",
    ST_AsGeoJSON(ST_Transform(b.geom, 4326)) :: json as \"geometry\",
    (
    select json_strip_nulls(row_to_json(t))
    from (
    select
    a.postcode,
    round(a.te19_prob, 2) as probability
    ) t
    ) as \"properties\"
      from model.probability_",
    tablesuffix,
    " as a
    LEFT JOIN data.postcode_polygons b ON a.postcode = b.postcode
    LEFT JOIN data.pc_pop_2011 c ON a.postcode = c.postcode
    where a.crscode = '",
    crs,
    "' and a.te19_prob > 0.01 and st_within(c.geom, (select service_area_60mins from model.proposed_stations where crscode = '", crs, "'))
    ) as f
    ) as fc ) where crscode = '", crs, "'"
  )
  getQuery(con, query)
}


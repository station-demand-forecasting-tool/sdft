#' Creates a GeoJSON probabilistic catchment for a station
#'
#' Creates a GeoJSON probabilistic catchment for a station. Includes a spatial
#' query to check that any candidate postcodes are within the 60 minute service area of the
#' station (only important for "concurrent" mode).
#'
#' If \code{abs_crs} is \code{NULL} then the catchment is
#' assumed to be for a proposed station and the table model.proposed_stations is
#' updated. If a value for \code{abs_crs} (a proposed station) is provided then the catchment is
#' assumed to relate to an abstraction analysis and is generated for either the before or
#' after situation depending on the value of \code{tablesuffix} and the table model.abstraction_analysis is
#' updated.
#'
#' @param crs Character. The crscode of the station the catchment is for.
#' @param tablesuffix Character. The suffix of the probability table.
#' @param abs_crs Character. The crscode of a proposed station (or "concurrent" if
#' using concurrent mode). If specified then either a
#' before or after catchment is generated (depending on value of \code{tablesuffix}).
#' @param cutoff Numeric. Defines the threshold probability to exclude a postcode from
#' the catchment. Default is 0.01 (i.e. any postcodes with a probability greater than 0.01
#' for the station defined in \code{crs} will be included in the catchment).
#' @export
sdr_create_json_catchment <- function(crs, tablesuffix, abs_crs = NULL, cutoff = 0.01) {


  if(is.null(abs_crs)) {
    update_table <- "model.proposed_stations"
    set_column <- "catchment"
    where_clause <- paste0("crscode = '", crs, "'")
    sa_table <- "model.proposed_stations"
  } else {
    if (grepl("before", tablesuffix, fixed = TRUE)) {
      set_column <- "catchment_before"
      } else {
        set_column <- "catchment_after"
      }
    update_table <- "model.abstraction_results"
    where_clause <- paste0("at_risk = '", crs, "' and proposed = '", abs_crs, "'")
    sa_table <- "data.stations"
  }


  query <- paste0(
    "update ", update_table, " set ", set_column, " = (select row_to_json(fc)
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
    "' and a.te19_prob > ", cutoff, " and st_within(c.geom, (select service_area_60mins from ", sa_table, " where crscode = '",
    crs,
    "'))
    ) as f
    ) as fc ) where ", where_clause
    )
  getQuery(con, query)
}


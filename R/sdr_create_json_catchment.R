#' Creates a GeoJSON probabilistic catchment for a station
#'
#' Creates a GeoJSON probabilistic catchment for a station. Includes a spatial
#' query to check that any candidate postcodes are within the 60 minute service area of the
#' station (only important for \emph{concurrent} mode as for \emph{isolation} mode only those postcodes
#' within 60 minutes of a station will be in that station's probability table).
#'
#' If \code{type} is set to "proposed" then the station defined by \code{crs} is assumed to be
#' a proposed station present in the modelschema.proposed_stations table. The 60-minute service area
#' for the station is expected to be in this table and the GeoJSON catchment is written to this table.
#'
#' If \code{type} is "abstraction" then the station defined by \code{crs} is assumed to be
#' an at-risk station that is part of an abstraction analysis. The 60-minute service area for the
#' station is assumed to be located in the data.stations table and the catchment is written to the
#' modelschema.abstraction_results table. If \code{abs_crs} is not specified then a \emph{before} catchment
#' is generated for station \code{crs}. If a proposed station is provided in \code{abs_crs} (or this
#' is given the value "concurrent" in the case of concurent mode) then an \emph{after} catchment is generated,
#' reflecting the situation after \code{abs_crs}.
#'
#' @param schema A text string for the database schema name.
#' @param type Character. Must be either "proposed" or "abstraction". Indicates whether this catchment is required for a proposed station
#' or as part of an abstraction analysis. See details.
#' @param crs Character. The crscode of the station the catchment is for.
#' @param tablesuffix Character. Suffix of the probability table (i.e. the part after "schema.probability_")
#' @param abs_crs Character. The crscode of a proposed station (or "concurrent" if
#' using concurrent mode). Only relevant if \code{type} is set to "abstraction". When specified an
#' \emph{after} catchment is generated for station \code{crs} with station \code{abs_crs} present.
#' @param cutoff Numeric. Defines the threshold probability below which to exclude a postcode from
#' the catchment. Default is 0.01 (i.e. any postcodes with a probability of \eqn{>= 0.01}
#' for the station defined in \code{crs} will be included in the catchment).
#' @export
sdr_create_json_catchment <- function(schema, type, crs, tablesuffix, abs_crs = NULL, cutoff = 0.01) {



  if (type == "proposed") {
    update_table <- paste0(schema, ".proposed_stations")
    set_column <- "catchment"
    where_clause <- paste0("crscode = '", crs, "'")
    sa_table <- paste0(schema, ".proposed_stations")
  } else if (type == "abstraction") {
    if (is.null(abs_crs)) {
      set_column <- "catchment_before"
      where_clause <- paste0("at_risk = '", crs, "'")
    } else {
      set_column <- "catchment_after"
      where_clause <-
        paste0("at_risk = '", crs, "' and proposed = '", abs_crs, "'")
    }
    update_table <- paste0(schema, ".abstraction_results")
    sa_table <- "data.stations"
  } else {
    stop("type not valid")
  }

  futile.logger::flog.info(paste0("creating GeoJSON catchment, updating table: ", update_table))
  futile.logger::flog.info(paste0("creating GeoJSON catchment, updating column: ", set_column))
  futile.logger::flog.info(paste0("creating GeoJSON catchment, using 60 minute sa from table: ", sa_table))
  futile.logger::flog.info(paste0("creating GeoJSON catchment, where: ", where_clause))

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
      from ", schema, ".probability_",
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
  sdr_dbExecute(con, query)
}


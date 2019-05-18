#' Creates and populates the probability table for a station
#'
#' Creates and populates the probability table for a station (isolation)
#' or stations (concurrent) with the variables required to apply
#' the station choice model. The variables are drawn from the
#' schema.proposed_stations and data.stations tables.
#'
#' @param schema Character. for the database schema name.
#' @param df A dataframe containing the choicesets.
#' @param tablesuffix Character, the suffix of the probability table - either
#' crscode (isolation) or 'concurrent' (concurrent) is expected.
#' @export
sdr_generate_probability_table <- function(schema, df, tablesuffix) {

  flog.info(paste0("Creating probability table: ", schema, ".probability_",
                   tablesuffix))

  query <- paste0(
    "create table ", schema, ".probability_",
    tablesuffix,
    "
    (
    id            serial primary key,
    postcode      text,
    crscode       text,
    distance      double precision,
    distance_rank smallint,
    sqr_dist      numeric,
    ln_dfreq      numeric,
    carspaces     int,
    category      text,
    nearest       int,
    cat_f         int,
    ticketmachine int,
    buses         int,
    cctv          int
    );
    "
  )
  sdr_dbExecute(con, query)

  # write the table for this crscode
  RPostgres::dbWriteTable(
    conn = con,
    Id(schema = schema, table = paste0("probability_",
                             tablesuffix)),
    df,
    append =
      TRUE,
    row.names = FALSE
  )

  # populate columns
  query <- paste0(
    "
    with tmp as (
    select crscode, frequency as frequency, carspaces, category, ticketmachine, busservices, cctv
    from data.stations
    union all
    select crscode, freq as frequency, carsp as carspaces, category, ticketm as ticketmachine, busint as busservices, cctv
    from ", schema, ".proposed_stations
    )
    UPDATE ", schema, ".probability_",
    tablesuffix,
    " as a SET
    sqr_dist = round(cast(sqrt(distance/1000) as numeric),4),
    ln_dfreq = round(cast(ln(b.frequency) as numeric),4),
    carspaces = b.carspaces,
    category = b.category,
    nearest = CASE WHEN distance_rank = '1' THEN 1 ELSE 0 END,
    cat_f = CASE WHEN b.category = 'F' OR b.category = 'F1' OR b.category = 'F2' THEN 1 ELSE 0 END,
    ticketmachine = CASE WHEN b.ticketmachine = 'true' THEN 1 WHEN b.ticketmachine = 'false' THEN 0 END,
    buses = CASE WHEN b.busservices = 'true' THEN 1 WHEN b.busservices = 'false' THEN 0 ELSE 0 END,
    cctv = CASE WHEN b.cctv = 'true' THEN 1 WHEN b.cctv = 'false' THEN 0 ELSE 0 END
    FROM tmp as b
    WHERE a.crscode = b.crscode
    "
  )
  sdr_dbExecute(con, query)
}

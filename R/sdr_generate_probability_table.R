#' Creates and populates the probability table of a station
#'
#' Creates and populates the probability table of a station (isolation)
#' or stations (concurrent) with the variables required to apply
#' the station choice model. The variables are drawn from the model.proposed_stations
#' and data.stations tables.
#'
#' @param df A dataframe containing the choicesets.
#' @param tablesuffix The suffix of the probability table - either crscode
#' (isolation) or 'concurrent' (concurrent) is expected.
#' @export
sdr_generate_probability_table <- function(df, tablesuffix) {

  query <- paste0(
    "create table model.probability_",
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
  getQuery(con, query)

  # write the table for this crscode
  RPostgreSQL::dbWriteTable(
    conn = con,
    name = c('model', paste0("probability_",
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
    select crscode, dailyfrequency_2017_all as frequency, carspaces, category, ticketmachine, busservices, cctv
    from data.stations
    union all
    select crscode, freq as frequency, carsp as carspaces, category, ticketm as ticketmachine, busint as busservices, cctv
    from model.proposed_stations
    )
    UPDATE model.probability_",
    tolower(tablesuffix),
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
  getQuery(con, query)
}
#' Adjusts the service frequency of stations based on frequency group
#'
#' Updates the probability table for a station (isolation) or
#' stations (concurrent), adjusting the frequency variable for existing stations
#' based on information provided in the frequency group input file.
#'
#' @param con An RPostgres database connection object.
#' @param schema Character, the database schema name.
#' @param df A data frame containing the frequency group input. In isolation mode
#' this will relate to a specific station. In concurrent mode there will only be
#' a single frequency group across all stations.
#' @param tablesuffix Character, the suffix of the probability table to be
#' updated - either a crscode (isolation) or 'concurrent' (concurrent) is expected.
#' @export
sdr_frequency_group_adjustment <-
  function(con, schema, df, tablesuffix) {
    # count number of stations in the frequency group
    freqgrp_no <- length(strsplit(df$fgrp, ",")[[1]])
    df <-
      data.frame(frgrp = t(df %>% tidyr::separate(
        .data$fgrp, c(letters[1:freqgrp_no]), sep = ",", fill = "right"
      )))
    df <-
      tidyr::separate(
        df,
        1,
        c("crs", "frequency"),
        fill = "right",
        sep = ":",
        convert = TRUE
      )


    for (crs in df$crs) {
      futile.logger::flog.info(paste0("Making frequency group adjustment for: ", crs))
      query <- paste0(
        "
      update ",
        schema,
        ".probability_",
        tolower(tablesuffix),
        "
      set ln_dfreq = round(cast(ln(",
        df$frequency[df$crs == crs],
        ") as numeric),4)
      where crscode = '",
        crs,
        "'
      "
      )
      sdr_dbExecute(con, query)

    }
  }

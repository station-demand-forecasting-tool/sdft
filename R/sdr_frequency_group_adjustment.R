sdr_frequency_group_adjustment <- function(df, tablesuffix) {
  # count number of stations in the frequency group
  freqgrp_no <- length(strsplit(df$fgrp, ",")[[1]])
  df <-
    data.frame(frgrp = t(df %>% separate(
      fgrp, c(letters[1:freqgrp_no]), sep = ",", fill = "right"
    )))
  df <-
    separate(
      df,
      1,
      c("crs", "frequency"),
      fill = "right",
      sep = ":",
      convert = TRUE
    )

  for (crs in df$crs) {
    query <- paste0(
      "
      update model.probability_",
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
    getQuery(con, query)

  }
}

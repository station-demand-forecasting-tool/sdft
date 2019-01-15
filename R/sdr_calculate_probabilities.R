sdr_calculate_probabilities <- function(tablesuffix) {
  # create probability columns

  query <-
    paste(
      "ALTER TABLE model.probability_",
      tolower(tablesuffix),
      "
      ADD COLUMN te19_expv numeric,
      ADD COLUMN te19_sum_expv numeric,
      ADD COLUMN te19_prob numeric
      "
      ,
      sep = ""
    )
  query <- gsub(pattern = '\\s',
                replacement = " ",
                x = query)
  getQuery(con, query)

  #calculate probability

  query <-
    paste0(
      "UPDATE model.probability_",
      tolower(tablesuffix),
      "
      SET te19_expv =
      exp(
      (nearest * .691) +
      (sqr_dist * -2.262) +
      (cat_f * -.677) +
      (ln_dfreq * 1.199) +
      (cctv * 1.071) +
      (carspaces * .001) +
      (ticketmachine * .984) +
      (buses * .758)
      )
      "
    )
  getQuery(con, query)

  query <-
    paste0(
      "UPDATE model.probability_",
      tolower(tablesuffix),
      " SET te19_sum_expv = b.sumexpv from
      (
      SELECT id, (sum(te19_expv) OVER (PARTITION BY postcode)) as sumexpv FROM model.probability_",
      tolower(crscode),
      "
      ) as b
      where b.id = model.probability_",
      tolower(crscode),
      ".id;
      "
      )
  getQuery(con, query)

  query <-
    paste(
      "UPDATE model.probability_",
      tolower(tablesuffix),
      "
      SET te19_prob =
      te19_expv / te19_sum_expv
      ",
      sep = ""
    )
  getQuery(con, query)

 # create indexes

  query <-
    paste(
      "
      create index on model.probability_",
      tolower(tablesuffix),
      " (crscode)
      ",
      sep = ""
    )
  getQuery(con, query)

  query <-
    paste(
      "
      create index on model.probability_",
      tolower(tablesuffix),
      " (postcode)
      ",
      sep = ""
    )
  getQuery(con, query)

  query <-
    paste(
      "
      create index on model.probability_",
      tolower(tablesuffix),
      " (distance)
      ",
      sep = ""
    )
  getQuery(con, query)

}

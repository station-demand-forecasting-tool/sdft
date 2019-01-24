#' Calculates probabilities for postcode choicesets
#'
#' Calculates the probability of each station being chosen within the postcode choicesets
#' contained in the specified probability table for the proposed station (isolation)
#' or stations (concurrent). The required columns are created in the table.
#' @param tablesuffix The suffix of the probability table - either crscode
#' (isolation) or 'concurrent' (concurrent) is expected.
#' @export


sdr_calculate_probabilities <- function(tablesuffix) {

  # --------+--------------------------------------------------------------------
  #         |                  Standard            Prob.      95% Confidence
  # CHOICE  |  Coefficient       Error       z    |z|>Z*         Interval
  # --------+--------------------------------------------------------------------
  # NEAREST |     .69065***      .03744    18.44  .0000      .61726    .76404
  # SQR_DIST|   -2.26183***      .04016   -56.31  .0000    -2.34056  -2.18311
  # CAT_F   |    -.67672***      .04226   -16.01  .0000     -.75954   -.59390
  # LN_DFQAL|    1.19857***      .03468    34.57  .0000     1.13061   1.26654
  # CCTV    |    1.07082***      .12464     8.59  .0000      .82652   1.31512
  # CPSPACES|     .00132***   .7988D-04    16.48  .0000      .00116    .00147
  # TICKETM |     .98392***      .05156    19.08  .0000      .88286   1.08497
  # BUSES   |     .75848***      .05574    13.61  .0000      .64924    .86773
  # --------+--------------------------------------------------------------------

  var_nearest <- .69065
  var_sqr_dist <- -2.26183
  var_cat_f <- -.67672
  var_ln_dfqal <- 1.19857
  var_cctv <- 1.07082
  var_cpspaces <- .00132
  var_ticketm <- .98392
  var_buses <- .75848

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
      (nearest * ", var_nearest ,") +
      (sqr_dist * ", var_sqr_dist ,") +
      (cat_f * ", var_cat_f ,") +
      (ln_dfreq * ", var_ln_dfqal ,") +
      (cctv * ", var_cctv ,") +
      (carspaces * ", var_cpspaces ,") +
      (ticketmachine * ", var_ticketm ,") +
      (buses * ", var_buses ,")
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

sdr_calculate_prweighted_population <- function(crs, tablesuffix) {

  # in summary gets the population for each postcode in which the proposed station
  # appears in its choice set and weights it by the probability of that station being
  # chosen for that postcodes and were relevant (distance to station > 750m) applies
  # a distance decay function. This then summed for all these postcodes.

  # there are 3 CTE queries involved
  # the first (nw_pop) is population weighted just by probability when distance to station <= 750m
  # the second (w_pop) is population weighted by probability AND by distance decay function
  # the third (adj_pop) gets the probability weighted population based on the exogenous data
  # table, applying the decay function for access distance > 750m

  query <- paste0(
    "
    WITH nw_pop AS(
    SELECT
    sum(a.te19_prob * b.population) FROM model.probability_",
    tolower(tablesuffix),
    " AS a
    LEFT JOIN data.pc_pop_2011 AS b ON a.postcode = b.postcode
    WHERE a.crscode = '",
    crs,
    "' AND a.distance <= 750
    ), w_pop AS (
    SELECT
    sum(a.te19_prob * b.population * power(((a.distance / 1000) +1), -1.5212)) FROM model.probability_",
    tolower(tablesuffix),
    " AS a
    LEFT JOIN data.pc_pop_2011 AS b ON a.postcode = b.postcode
    WHERE a.crscode = '",
    crs,
    "' AND a.distance > 750
    ), adj_pop as (
    select sum (
    case when b.distance <=750 then b.te19_prob * a.population
    when b.distance > 750 then b.te19_prob * a.population * power(((b.distance / 1000) +1), -1.5212)
    end)
    from model.exogenous_input as a
    left join model.probability_",
    tolower(tablesuffix),
    " as b
    on a.centroid = b.postcode and b.crscode = '",
    crs,
    "'
    where type = 'population' or type = 'houses'
    )
    update model.proposed_stations set prw_pop =
    (SELECT round(COALESCE(nw_pop.sum, 0) + COALESCE(w_pop.sum, 0)) AS w_pop FROM nw_pop, w_pop)
    where crscode = '",
    crs,
    "'
    "
    )
  getQuery(con, query)

}

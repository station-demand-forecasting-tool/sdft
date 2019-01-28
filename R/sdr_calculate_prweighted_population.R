#' Calculates the total probability weighted population for a station
#'
#' Calculates the total probability weighted population for a station
#' by weighting the population of each postcode by the choice
#' probability for the station and a distance decay function and then summing
#' across all postcodes. In addition takes account of population data in the
#' exogenous inputs table.
#'
#' The calculation is different depending on whether the concurrent or isolation
#' method is to be used. For concurrent treatment there is only a single probability
#' table and a spatial query is used so that only those postcodes within the 60 minutes
#' service area of an individual proposed station are included. This is because a single merged
#' service area (within 60 minutes of all the stations) was used to generate the postcodes
#' in the probability table. But only those within 60 minutes of each proposed station
#' should be considered when generating the weighted population. It is also necessary
#' to only include the exogenous population for postcodes that fall within a
#' station's 60-minute service area.
#'
#' There are 3 CTE queries involved. The first (nw_pop) is population weighted just
#' by probability when the distance to a station is <= 750m. The second (w_pop) is population
#' weighted by probability AND by distance decay function. The third (adj_pop) gets
#' the probability weighted population based on the exogenous data table, applying
#' the decay function for access distances > 750m.
#'
#' For stations treated in isolation the FROM table for nw_pop and w_pop is the probability table for that
#' station and census postcode population is joined by a left join, so only relevant
#' postcodes will ever be included. In the case of adj_pop the from table is the exogenous table
#' and the probability is left joined from the probability table. This will be null if the postcode
#' is not present in the probability table and it will not in those circumstances contribute to the sum.
#' As in SQL NULL times anything is NULL.
#'
#' @param crs The crscode of the station.
#' @param tablesuffix The suffix of the probability table - either crscode
#' (isolation) or 'concurrent' (concurrent) is expected.
#' @export
sdr_calculate_prweighted_population <- function(crs, tablesuffix) {


  if (tablesuffix == "concurrent") {
    query <- paste0(
      "
      with nw_pop as(
      select
      sum(a.te19_prob * b.population) from model.probability_",
      tolower(tablesuffix),
      " as a
      left join data.pc_pop_2011 as b on a.postcode = b.postcode
      left join model.proposed_stations as c on a.crscode = c.crscode
      where a.crscode = '",
      crs,
      "' and a.distance <= 750 and st_within(b.geom, c.service_area_60mins)
      ), w_pop as (
      select
      sum(a.te19_prob * b.population * power(((a.distance / 1000) +1), -1.5212)) from model.probability_",
      tolower(tablesuffix),
      " as a
      left join data.pc_pop_2011 as b on a.postcode = b.postcode
      left join model.proposed_stations as c on a.crscode = c.crscode
      where a.crscode = '",
      crs,
      "' and a.distance > 750 and st_within(b.geom, c.service_area_60mins)
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
      and st_within (a.geom, (select service_area_60mins from model.proposed_stations where crscode = '",
      crs,
      "'))
      )
      select round(coalesce(nw_pop.sum, 0) + coalesce(w_pop.sum, 0) + coalesce(adj_pop.sum, 0)) as w_pop from nw_pop, w_pop, adj_pop
      "
      )
    result <- getQuery(con, query)
  } else {
    query <- paste0(
      "
      with nw_pop as(
      select
      sum(a.te19_prob * b.population) from model.probability_",
      tolower(tablesuffix),
      " as a
      left join data.pc_pop_2011 as b on a.postcode = b.postcode
      where a.crscode = '",
      crs,
      "' and a.distance <= 750
      ), w_pop as (
      select
      sum(a.te19_prob * b.population * power(((a.distance / 1000) +1), -1.5212)) from model.probability_",
      tolower(tablesuffix),
      " as a
      left join data.pc_pop_2011 as b on a.postcode = b.postcode
      where a.crscode = '",
      crs,
      "' and a.distance > 750
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
      select round(coalesce(nw_pop.sum, 0) + coalesce(w_pop.sum, 0) + coalesce(adj_pop.sum, 0)) as w_pop from nw_pop, w_pop, adj_pop
      "
      )
    result <- getQuery(con, query)
  }

return(result)
}

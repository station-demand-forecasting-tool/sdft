% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sdr_calculate_prweighted_population.R
\name{sdr_calculate_prweighted_population}
\alias{sdr_calculate_prweighted_population}
\title{Calculates the total probability weighted population for a station}
\usage{
sdr_calculate_prweighted_population(con, schema, crs, tablesuffix)
}
\arguments{
\item{con}{An RPostgres database connection object.}

\item{schema}{Character, the database schema name.}

\item{crs}{Character, the crscode of the station.}

\item{tablesuffix}{Character, the suffix of the probability table - either
crscode (isolation) or 'concurrent' (concurrent) is expected.}
}
\description{
Calculates the total probability weighted population for a station
by weighting the population of each postcode by the choice
probability for the station and a distance decay function and then summing
across all postcodes. In addition, takes account of population data in the
exogenous inputs table.
}
\details{
The calculation is different depending on whether the concurrent or isolation
method is to be used. For concurrent treatment there is only a single
probability table and a spatial query is used so that only those postcodes
within the 60 minute service area of an individual proposed station are
included. This is because a single merged service area (within 60 minutes of
all the stations) was used to generate the postcodes in the probability table.
But only those within 60 minutes of each proposed station should be considered
when generating the weighted population. It is also necessary to only include
the exogenous population for postcodes that fall within a station's 60-minute
service area.

There are three CTE queries involved. The first (nw_pop) is population weighted
just by probability when the distance to a station is <= 750m. The second (w_pop)
is population weighted by probability AND by distance decay function. The third
(adj_pop) gets the probability weighted population based on the exogenous data
table, applying the decay function for access distances > 750m.

For stations treated in isolation the FROM table for nw_pop and w_pop is the
probability table for that station and census postcode population is joined
by a left join, so only relevant postcodes will ever be included. In the case
of adj_pop the from table is the exogenous table and the probability is left
joined from the probability table. This will be null if the postcode is not
present in the probability table and it will not in those circumstances
contribute to the sum - as in SQL, NULL times anything is NULL.
}

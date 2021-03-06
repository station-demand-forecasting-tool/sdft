% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sdr_calculate_probabilities.R
\name{sdr_calculate_probabilities}
\alias{sdr_calculate_probabilities}
\title{Calculates the probabilities for postcode choicesets}
\usage{
sdr_calculate_probabilities(con, schema, tablesuffix)
}
\arguments{
\item{con}{An RPostgres database connection object.}

\item{schema}{Character, the database schema name.}

\item{tablesuffix}{Character, the suffix of the probability table - either
crscode (isolation) or 'concurrent' (concurrent) is expected.}
}
\description{
Calculates the probability of each station being chosen within the postcode
choicesets contained in the specified probability table for the proposed
station (isolation) or stations (concurrent). The required columns are
created in the table.
}

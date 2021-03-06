% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sdr_create_json_catchment.R
\name{sdr_create_json_catchment}
\alias{sdr_create_json_catchment}
\title{Creates a GeoJSON probabilistic catchment for a station}
\usage{
sdr_create_json_catchment(
  con,
  schema,
  type,
  pcpoly = FALSE,
  crs,
  tablesuffix,
  abs_crs = NULL,
  cutoff = 0.01,
  tolerance = 0.1
)
}
\arguments{
\item{con}{An RPostgres database connection object.}

\item{schema}{Character, the database schema name.}

\item{type}{Character, must be either "proposed" or "abstraction". Indicates
whether this catchment is required for a proposed station or as part of an
abstraction analysis. See details.}

\item{pcpoly}{Logical. If TRUE the postcode polygon dataset is
available and will be used for the catchment. If FALSE, the catchment will
just consist of the postcode centroids.  Default is FALSE.}

\item{crs}{Character, the crscode of the station the catchment is for.}

\item{tablesuffix}{Character, suffix of the probability table (i.e. the part
after "schema.probability_")}

\item{abs_crs}{Character, the crscode of a proposed station (or "concurrent"
if using concurrent mode). Only relevant if \code{type} is set to
"abstraction". When specified an \emph{after} catchment is generated for
station \code{crs} with station \code{abs_crs} present.}

\item{cutoff}{Numeric, defines the threshold probability below which to
exclude a postcode from the catchment. Default is 0.01 (i.e. any postcodes
with a probability of \eqn{>= 0.01} for the station defined in \code{crs}
will be included in the catchment).}

\item{tolerance}{Numeric, tolerance for ST_SimplifyPreserveTopology. Default
is 0.1.}
}
\description{
Creates a GeoJSON probabilistic catchment for a station. Includes a spatial
query to check that any candidate postcodes are within the 60 minute service
area of the station (only important for \emph{concurrent} mode as for
\emph{isolation} mode only those postcodes within 60 minutes of a station
will be in that station's probability table).
}
\details{
If \code{type} is set to "proposed" then the station defined by \code{crs}
is assumed to be a proposed station present in the schema.proposed_stations
table. The 60-minute service area for the station is expected to be in this
table and the GeoJSON catchment is also written to this table.

If \code{type} is "abstraction" then the station defined by \code{crs} is
assumed to be an at-risk station that is part of an abstraction analysis. The
60-minute service area for the station is assumed to be located in the
data.stations table and the catchment is written to the
schema.abstraction_results table. If \code{abs_crs} is not specified then a
\emph{before} catchment is generated for station \code{crs}. If a proposed
station is provided in \code{abs_crs} (or this is given the value "concurrent"
in the case of concurrent mode) then an \emph{after} catchment is generated,
reflecting the situation after \code{abs_crs}.
}

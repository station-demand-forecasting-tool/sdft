# requires: dplyr, tidyr, foreach and doParallel libraries

library(dplyr)
library(tidyr)
library(foreach)
library(parallel)
library(doParallel)

# load configuration data-------------------------------------------------------

config <-
  read.csv(file = "inst/example_input/config.csv",
           sep = ";",
           stringsAsFactors = FALSE)

isolation <- ifelse(config$method == "isolation", TRUE, FALSE)


# load station input data-------------------------------------------------------

stations <-
  read.csv(file = "inst/example_input/stations.csv",
           sep = ";",
           stringsAsFactors = FALSE)


# create location column
stations$location <-
  paste0(stations$acc_east, ",", stations$acc_north)
colnames(stations)[1] <- "crscode"


# Load exogenous data-----------------------------------------------------------

exogenous <-
  read.csv(file = "inst/example_input/exogenous.csv",
           sep = ";",
           stringsAsFactors = FALSE)



# database setup----------------------------------------------------------------


# create db schema to hold model data

schema <- "model"

query <- paste0("
                create schema ", schema, " authorization postgres;
                ")
getQuery(con, query)


# write data.frame of proposed stations to postgresql table

dbWriteTable(
  conn = con,
  name = c(schema, 'proposed_stations'),
  stations,
  append =
    FALSE,
  row.names = TRUE
)

query <- paste0("
                alter table model.proposed_stations rename \"row.names\" TO id;
                ")
getQuery(con, query)

query <- paste0("
                alter table model.proposed_stations alter column id type int
                using id::integer;
                ")
getQuery(con, query)



# create geometry column in proposed_stations table

query <- paste0(
  "
  alter table ",
  schema,
  ".proposed_stations add column location_geom geometry(Point,27700);
  "
)
getQuery(con, query)


# populate location_geom

query <- paste0(
  "
  update ",
  schema,
  ".proposed_stations set location_geom =
  ST_GeomFromText('POINT('||stn_east||' '||stn_north||')', 27700);
  "
)
getQuery(con, query)


# Create station service areas--------------------------------------------------


# create distance-based service areas used in identifying nearest 10 stations to
# each postcode centroid

sdr_create_service_areas(
  df = stations,
  schema = schema,
  table = "proposed_stations",
  sa = c(1000, 5000, 10000, 20000, 30000, 40000, 60000, 80000, 105000),
  cost = "len"
)

# create 60 minute service area - used to identify postcode centroids to be
# considered for inclusion in model

sdr_create_service_areas(
  df = stations,
  schema = schema,
  table = "proposed_stations",
  sa = c(60),
  cost = "time",
  target = 0.9
)

# create 1 minute service area - used to identify number of jobs within 1 minute
# of station

sdr_create_service_areas(
  df = stations,
  schema = schema,
  table = "proposed_stations",
  sa = c(1),
  cost = "time",
  target = 0.9
)


# create probability table------------------------------------------------------

# set up for parallel processing

cl <- makeCluster(detectCores() - 2)
registerDoParallel(cl)

clusterEvalQ(cl, {
  library(DBI)
  library(RPostgreSQL)
  library(keyring)
  getQuery <- function(con, query) {
    query <- gsub(pattern = '\\s' ,
                  replacement = " ",
                  x = query)
    dbGetQuery(con, query)
  }
  drv <- dbDriver("PostgreSQL")
  con <-
    dbConnect(
      drv,
      host = "localhost",
      user = "postgres",
      password = key_get("postgres"),
      dbname = "dafni"
    )
  NULL
})

if (isolation) {
  for (crscode in stations$crscode) {
    choicesets <- sdr_generate_choicesets_parallel(crscode)

    query <- paste0(
      "create table model.probability_",
      tolower(crscode),
      "
      (
      id            serial primary key,
      postcode      text,
      crscode       text,
      distance      double precision,
      distance_rank smallint
      );
      "
    )
    getQuery(con, query)

    dbWriteTable(
      conn = con,
      name = c('model', paste0("probability_",
                               tolower(crscode))),
      choicesets,
      append =
        TRUE,
      row.names = FALSE
    )

  }
} else {
  choicesets <- sdr_generate_choicesets_parallel(stations$crscode)

  query <- paste0(
    "create table model.probability_concurrent
    (
    id            serial primary key,
    postcode      text,
    crscode       text,
    distance      double precision,
    distance_rank smallint
    );
    "
  )
  getQuery(con, query)

  dbWriteTable(
    conn = con,
    name = c('model', 'probability_concurrent'),
    choicesets,
    append =
      TRUE,
    row.names = FALSE
  )
}

stopCluster(cl)


# populate probability table(s)-------------------------------------------------


if (isolation) {
  for (crscode in stations$crscode) {

    # populate probability table
    sdr_populate_probability_table(crscode)

    # make frequency group adjustments if required
    if (!is.na(stations$freqgrp[stations$crscode == crscode])) {
      df <-
        data.frame(fgrp = config[config$group_id == stations$freqgrp[stations$crscode == crscode], "group_crs"], stringsAsFactors = FALSE)
      sdr_frequency_group_adjustment(df, crscode)

    } # end if freqgrp

    # calculate probabilities
    sdr_calculate_probabilities(crscode)

    }
} else {

  # populate probability table
  sdr_populate_probability_table("concurrent")

  # make frequency group adjustments if required
  # must only be a single frequency group for concurrent treatment
  # So we just check the first row for the group name and process once

  if (!is.na(stations$freqgrp[1])) {
    df <-
      data.frame(fgrp = config[config$group_id == stations$freqgrp[1], "group_crs"], stringsAsFactors = FALSE)

    sdr_frequency_group_adjustment(df, "concurrent")

    }

  # calculate probabilities
  sdr_calculate_probabilities("concurrent")

}

# trip end model----------------------------------------------------------------





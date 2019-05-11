# This script creates a series of distance-based service areas for each railway station
# It expects the stations table to be in a schema called 'data'.
# This will take a considerable time to complete (24 hours perhaps).
# Service areas will be used when the nearest stations to each postcode centroid need
# to be generated.

# Preliminaries-----------------------------------------------------------------

library(stationdemandr)
library(dplyr)
library(tidyr)
library(readr)
library(foreach)
library(stringr)
library(doParallel)
library(keyring)
library(RPostgres)
library(DBI)
library(futile.logger)
library(checkmate)

# set up logging

# delete existing log files
if (file.exists("sdr.log")) {
  file.remove("sdr.log")
}

# delete existing log file
if (file.exists("sa.log")) {
  file.remove("sa.log")
}

threshold <- "INFO" # DEBUG, INFO, WARN, ERROR, FATAL
flog.appender(appender.file("sdr.log"))
# set logging level
flog.threshold(threshold)


# capture R errors and warnings to be logged by futile.logger
options(
  showWarnCalls = TRUE,
  showErrorCalls = TRUE,
  show.error.locations = TRUE,
  error = function() {
    flog.error(geterrmessage())
  },
  warning.expression =
    quote({
      if (exists("last.warning", baseenv()) && !is.null(last.warning)) {
        txt = paste0(names(last.warning), collapse = " ")
        flog.warn(txt)
      }
    })
)

# Set up a database connection.
# Using keyring package for storing database password in OS credential store
# to avoid exposing on GitHub. Amend as appropriate.

checkdb <- try(con <-
                 dbConnect(
                   RPostgres::Postgres(),
                   dbname = "dafni",
                   host = "localhost",
                   user = "postgres",
                   password = key_get("postgres")
                 ))
if (class(checkdb) == "try-error") {
  stop("Database connection has not been established")
}

# Set up parallel processing
# This is currently used in the sdr_create_service_areas() and
# sdr_generate_choicesets() functions, in a foreach loop.

# Number of clusters is total available cores less two.
cl <- makeCluster(detectCores() - 2)
registerDoParallel(cl)

checkcl <- try(clusterEvalQ(cl, {
  library(DBI)
  library(RPostgres)
  library(keyring)
  library(stationdemandr)
  drv <- dbDriver("Postgres")
  con <-
    dbConnect(
      RPostgres::Postgres(),
      host = "localhost",
      user = "postgres",
      password = key_get("postgres"),
      dbname = "dafni"
    )
  NULL
}))
if (class(checkcl) == "try-error") {
  stop("clusterEvalQ failed")
}


# create stations dataframe - populate with crscodes and location coordinates
query <- paste0("select crscode, round(st_x(location_geom)) || ',' || round(st_y(location_geom)) as location from data.stations")
stations <- dbGetQuery(con, query)


sdr_create_service_areas(
  schema = "data",
  df = stations,
  identifier = "crscode",
  table = "station",
  sa = c(1000, 5000, 10000, 20000, 30000, 40000, 60000, 80000, 105000),
  cost = "len"
)

# 5 minute is to use as fake 60 minute in testing mode

sdr_create_service_areas(
  schema = "data",
  df = stations,
  identifier = "crscode",
  table = "stations",
  columns = TRUE,
  sa = c(5,60),
  cost = "time"
)



# create spatial indexes for the service areas

sa_names <- c("service_area_1km",
              "service_area_5km",
              "service_area_10km",
              "service_area_20km",
              "service_area_30km",
              "service_area_40km",
              "service_area_60km",
              "service_area_80km",
              "service_area_105km",
              "service_area_60mins")

for (sa_name in sa_names) {
  query <- paste0("create index idx_stations_", sa_name,
                  " on data.stations using gist(", sa_name, ")")
  sdr_dbExecute(con, query)
}

# create 'actual' 60 minute service area for swap between testing modes.

query <- "alter table data.stations add column service_area_60mins_actual geometry(polygon,27700);"
sdr_dbExecute(con, query)

query <- "update data.stations set service_area_60mins_actual = service_area_60mins"
sdr_dbExecute(con, query)

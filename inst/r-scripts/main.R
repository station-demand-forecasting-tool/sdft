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


# delete existing log file
if (file.exists("sdr.log")) {
  file.remove("sdr.log")
}

if (file.exists("sa.log")) {
  file.remove("sa.log")
}

# set up logging
flog.appender(appender.file("sdr.log"))
# set logging level
flog.threshold("INFO")    # TRACE, DEBUG, INFO, WARN, ERROR, FATAL

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

# During testing set this variable to TRUE. This produces fake 60-minute
# proposed station service areas which are actually only 5-minute service areas.
testing <- FALSE
flog.info(paste0("Testing mode: ", ifelse(isTRUE(testing), "ON", "OFF")))

# Set up a database connection.
# Using keyring package for storing database password in Windows credential store
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
# sdr_generate_choicesets() functions, in the foreach loop.

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

# Get station crscodes from data.stations for later validation

query <- paste0("
                select crscode from data.stations
                ")
crscodes <- sdr_dbGetQuery(con, query)

# Load input data-------------------------------------------------------

# file path

path <-
  file.path("inst", "example_input", fsep = .Platform$file.sep)

# check that the input files exist and can be read
file.coll <- checkmate::makeAssertCollection()
assert_file_exists(
  file.path(path, "config.csv", fsep = .Platform$file.sep),
  access = "r",
  add = file.coll
)
assert_file_exists(
  file.path(path, "freqgroups.csv", fsep = .Platform$file.sep),
  access = "r",
  add = file.coll
)
assert_file_exists(
  file.path(path, "stations.csv", fsep = .Platform$file.sep),
  access = "r",
  add = file.coll
)
assert_file_exists(
  file.path(path, "exogenous.csv", fsep = .Platform$file.sep),
  access = "r",
  add = file.coll
)
checkmate::reportAssertions(file.coll)


# load config.csv
config <-
  read.csv(
    file = "inst/example_input/config.csv",
    sep = ";",
    stringsAsFactors = FALSE,
    na.strings = c("")
  )

# load freqgroups.csv
freqgroups <-
  read.csv(file = "inst/example_input/freqgroups.csv",
           sep = ";",
           stringsAsFactors = FALSE)

# load stations.csv
stations <-
  read.csv(
    file = "inst/example_input/stations.csv",
    sep = ";",
    colClasses = c(
      "stn_east" = "character",
      "stn_north" = "character",
      "acc_east" = "character",
      "acc_north" = "character"
    ),
    stringsAsFactors = FALSE,
    na.strings = c("")
  )

# create location column in stations (convert to numeric)
stations$location <-
  paste0(as.numeric(stations$acc_east),
         ",",
         as.numeric(stations$acc_north))
# rename id column
colnames(stations)[1] <- "crscode"

# load exogenous.csv
exogenous <-
  read.csv(file = "inst/example_input/exogenous.csv",
           sep = ";",
           stringsAsFactors = FALSE)

flog.info("Input files read")


# Pre-flight checks-------------------------------------------------------------

preflight_failed <- FALSE

# check for valid mode
if (config$method == "isolation") {
  isolation <- TRUE
} else if (config$method == "concurrent") {
  isolation <- FALSE
} else {
  preflight_failed <- TRUE
  flog.error("Model method not valid, must be \"isolation\" or \"concurrent\"")
}

# set schema name to job_id. Check begins with a-z, then a-z or 0-9 or _ for an
# additional 4 matches only. Ensure valid postgresql schema format.
if (grepl("^[a-z][a-z0-9_]{1,5}$", config$job_id, ignore.case = FALSE)) {
  schema <- config$job_id
} else {
  preflight_failed <- TRUE
  flog.error(
    "database schema name uses config$job_id, but it is not in a valid format to
    be used as a schema name."
  )
}

# check frequency group format is ok
fg_pairs <- unlist(strsplit(freqgroups$group_crs, ","))
# check if all pairs match the required format
if (isFALSE(all(sapply(fg_pairs, function(x)
  grepl("^[A-Z]{3}:[0-9]{1,}$", x), USE.NAMES = FALSE)))) {
  preflight_failed <- TRUE
  flog.error("format of frequency groups is not correct")
}

# check crscodes in frequency groups are all valid
# get the unique crscodes from pairs
fg_crs <-
  unique(sapply(unlist(fg_pairs), function(x)
    sub(":.*", "", x), USE.NAMES = FALSE))
# get the index for those not valid
idx <- which(!(fg_crs %in% crscodes$crscode))
if (length(idx > 0)) {
  preflight_failed <- TRUE
  flog.error(paste(
    "The following frequency group crscodes are not valid : ",
    paste(fg_crs[idx], collapse = ", ")
  ))
}

# check crscode is unique
if (anyDuplicated(stations$crscode) > 0) {
  preflight_failed <- TRUE
  flog.error("station id must be unique")
}

# check abstraction station format is correct, if not NA
# i.e. check for CSV input of three uppercase characters separated by commas
# (also allowing single crscode with no comma). Maximum of three crscodes.
abs_check <-
  sapply(na.omit(stations$abstract), function(x)
    grepl("^([A-Z]{3})(,[A-Z]{3}){0,2}$", x), USE.NAMES = FALSE)
if (isFALSE(all(abs_check))) {
  preflight_failed <- TRUE
  flog.error("abstraction stations are not provided in the correct format")
}

# check that abstraction stations (if there are any) are valid crscodes
if (length(unique(na.omit(stations$abstract))) > 0) {
  abs_crs <- unique(na.omit(unlist(strsplit(
    stations$abstract, ","
  ))))
  # get the index for those not valid
  idx <- which(!(abs_crs %in% crscodes$crscode))
  if (length(idx > 0)) {
    preflight_failed <- TRUE
    flog.error(paste(
      "The following abstraction group crscodes are not valid: ",
      paste(abs_crs[idx], collapse = ", ")
    ))
  }
}

# check that frequency and number of parking spaces are integer > 0
if (isFALSE(testIntegerish(stations$freq, lower = 1, any.missing = FALSE))) {
  preflight_failed <- TRUE
  flog.error("service frequency must be integer > 0")
}
if (isFALSE(testIntegerish(stations$carsp, lower = 1, any.missing = FALSE))) {
  preflight_failed <- TRUE
  flog.error("parking spaces must be integer > 0")
}

# if concurrent
# check name is unique
# check abstraction string is same for all stations (even if NA)
# check same frequency group id is specified for every station (even if NA).
if (isFALSE(isolation)) {
  if (anyDuplicated(stations$name) > 0) {
    preflight_failed <- TRUE
    flog.error("station name must be unique for concurrent mode")
  }
  if (length(unique(stations$abstract)) > 1) {
    preflight_failed <- TRUE
    flog.error("defined abstraction stations must be identical for all stations
               in concurrent mode")
  }
  if (length(unique(stations$freqgrp)) > 1) {
    preflight_failed <- TRUE
    flog.error(
      "When using concurrent mode the same frequency group must be specified for
      every station"
    )
  }
}

# if isolation
# check that repeated station names only differ in id, freq, freqgrp, carsp
if (isTRUE(isolation)) {
  # if remove columns that are allowed to change
  if (nrow(stations %>% select(-crscode, -freq, -freqgrp, -carsp) %>%
           # distinct should only return as many rows as there are unique
           # station names
           distinct()) != length(unique(stations$name))) {
    preflight_failed <- TRUE
    flog.error(
      "In isolation mode stations with the same name can only differ
      in the values of id, freq, freqgrp, and carsp"
    )
  }
  }

# check exogenous number column is integer for all rows
if (!testIntegerish(exogenous$number, any.missing = FALSE)) {
  preflight_failed <- TRUE
  flog.error("exogenous number must all be integers")
}

# check valid centroids are used for the exogenous inputs

# If type is ‘population’ or ‘housing’ the value can only be assigned to a
# postcode centroid.

check_pc_centroids <- function(centroid) {
  query <- paste0("select '",
                  centroid,
                  "' IN (select postcode from data.pc_pop_2011)")
  as.logical(sdr_dbGetQuery(con, query))
}

pop_houses <-
  exogenous %>% filter(type == "population" | type == "houses")
idx <-
  which(
    sapply(pop_houses$centroid, function(x)
      check_pc_centroids(x), USE.NAMES = FALSE) == FALSE
  )
if (length(idx) > 0) {
  preflight_failed <- TRUE
  flog.error(
    paste0(
      "The following postcode centroids in the exogenous input are not valid: ",
      paste0(pop_houses$type[idx], ": ", pop_houses$centroid[idx], collapse = ", ")
    )
  )
}

# If type is ‘jobs’ the value can be assigned to either a postcode centroid or a
# workplace centroid.
check_wp_centroids <- function(centroid) {
  query <- paste0(
    "select '",
    centroid,
    "' IN (select wz from data.workplace2011 union select postcode from data.pc_pop_2011)"
  )
  as.logical(sdr_dbGetQuery(con, query))
}

jobs <- exogenous %>% filter(type == "jobs")
idx <-
  which(sapply(jobs$centroid, function(x)
    check_wp_centroids(x), USE.NAMES = FALSE) == FALSE)
if (length(idx) > 0) {
  preflight_failed <- TRUE
  flog.error(
    paste0(
      "The following workplace centroids in the exogenous input are not valid: ",
      paste0(jobs$type[idx], ": ", jobs$centroid[idx], collapse = ", ")
    )
  )
}

# station and access coordinates must be six digit integers
if (isFALSE(all(sapply(stations$stn_east, function(x)
  (
    grepl("^[0-9]{6}$", x)
  ))))) {
  preflight_failed <- TRUE
  flog.error("station eastings must all be 6 character strings containing 0-9 only")
}
if (isFALSE(all(sapply(stations$stn_north, function(x)
  (
    grepl("^[0-9]{6}$", x)
  ))))) {
  preflight_failed <- TRUE
  flog.error("station northings must all be 6 character strings containing 0-9 only")
}
if (isFALSE(all(sapply(stations$acc_east, function(x)
  (
    grepl("^[0-9]{6}$", x)
  ))))) {
  preflight_failed <- TRUE
  flog.error("access eastings must all be 6 character strings containing 0-9 only")
}
if (isFALSE(all(sapply(stations$acc_north, function(x)
  (
    grepl("^[0-9]{6}$", x)
  ))))) {
  preflight_failed <- TRUE
  flog.error("access northings must all be 6 character strings containing 0-9 only")
}

# check if access coordinates fall within GB extent
check_coords <- function(coords) {
  query <- paste0(
    "
    select st_intersects(st_setsrid(st_makepoint(",
    coords,
    "),27700), geom) from data.gb_outline;
    "
    )
  as.logical(sdr_dbGetQuery(con, query))
}

idx <-
  which(sapply(stations$location, function(x)
    check_coords(x), USE.NAMES = FALSE) == FALSE)
if (length(idx) > 0) {
  preflight_failed <- TRUE
  flog.error(paste0(
    "The following station access points do not fall within GB: ",
    paste0(stations$crscode[idx], ": ", stations$location[idx], collapse = ", ")
  ))
}

# stop if any of the pre-flight checks have failed
if (isTRUE(preflight_failed)) {
  stop("Pre-flight checks have failed")
} else {
  flog.info("Pre-flight checks passed")
}


# Database setup----------------------------------------------------------------

# create db schema to hold model data

query <- paste0("
                create schema ", schema, " authorization postgres;
                ")
sdr_dbExecute(con, query)


# write data.frame of proposed stations to postgresql table

dbWriteTable(
  conn = con,
  Id(schema = schema, table = "proposed_stations"),
  stations,
  append =
    FALSE,
  row.names = TRUE
)

query <- paste0("
                alter table ",
                schema,
                ".proposed_stations rename \"row_names\" TO id
                ")
sdr_dbExecute(con, query)

query <- paste0("
                alter table ",
                schema,
                ".proposed_stations alter column id type int
                using id::integer
                ")
sdr_dbExecute(con, query)

query <- paste0("
                alter table ", schema, ".proposed_stations add primary key (id)
                ")
sdr_dbExecute(con, query)

# create geometry column in proposed_stations table

query <- paste0(
  "
  alter table ",
  schema,
  ".proposed_stations add column location_geom geometry(Point,27700)
  "
)
sdr_dbExecute(con, query)

# populate location_geom

query <- paste0(
  "
  update ",
  schema,
  ".proposed_stations set location_geom =
  ST_GeomFromText('POINT('||stn_east||' '||stn_north||')', 27700)
  "
)
sdr_dbExecute(con, query)


flog.info("proposed_stations table successfully created")


# Prepare exogenous inputs----------------------------------------------

dbWriteTable(
  conn = con,
  Id(schema = schema, table = "exogenous_input"),
  exogenous,
  append =
    FALSE,
  row.names = TRUE
)

query <- paste0("
                alter table ",
                schema,
                ".exogenous_input rename \"row_names\" TO id
                ")
sdr_dbExecute(con, query)

query <- paste0("
                alter table ",
                schema,
                ".exogenous_input alter column id type int
                using id::integer
                ")
sdr_dbExecute(con, query)

query <- paste0("
                alter table ", schema, ".exogenous_input add primary key (id)
                ")
sdr_dbExecute(con, query)


# Add and populate geom column for the exogenous centroids.
# Can be either a postcode centroid or a workplace centroid

query <- paste0("
                alter table ",
                schema,
                ".exogenous_input
                add column geom geometry(Point,27700)
                ")
sdr_dbExecute(con, query)

query <- paste0(
  "
  with tmp as (
  select a.centroid, coalesce(b.geom, c.geom) as geom
  from ",
  schema,
  ".exogenous_input a
  left join data.pc_pop_2011 b on a.centroid = b.postcode
  left join data.workplace2011 c on a.centroid = c.wz
  )
  update ",
  schema,
  ".exogenous_input a
  set geom =
  (select distinct on (centroid) geom from tmp where a.centroid = tmp.centroid)
  "
)
sdr_dbExecute(con, query)

# Create columns and populate data that will be used for adjusting postcode
# probability weighted population

# population column
query <- paste0(
  "
  alter table ",
  schema,
  ".exogenous_input
  add column population int8,
  add column avg_hhsize numeric
  "
)
sdr_dbExecute(con, query)

# Copy from number to population for type 'population'
query <- paste0("
                update ",
                schema,
                ".exogenous_input set population = number where type
                ='population'
                ")
sdr_dbExecute(con, query)


# For type 'houses' we get the average household size for the local authority
# in which the postcode is located then calculate population and then populate
# the population column and avg_hhsize column.
query <- paste0(
  "
  with tmp as (
  select a.id, c.avg_hhsize_2019, a.number * c.avg_hhsize_2019 as population
  from ",
  schema,
  ".exogenous_input a
  left join data.pc_pop_2011 b
  on a.centroid = b.postcode
  left join data.hhsize c
  on b.oslaua = c.area_code
  where type = 'houses')
  update ",
  schema,
  ".exogenous_input a
  set population = tmp.population,
  avg_hhsize = tmp.avg_hhsize_2019
  from tmp
  where a.id = tmp.id
  "
)
sdr_dbExecute(con, query)

flog.info("exogenous table successfully created")


# Create station service areas--------------------------------------------------

# create distance-based service areas used in identifying nearest 10 stations to
# each postcode centroid

flog.info("starting to create station service areas")

# Because there may be duplicate stations of the same name for sensitivity analysis
# we create the service areas for each unique station name in a separate table and then
# update the proposed_stations table from that. While a bit more complex this removes
# unnecessary duplication of what can be a relatively slow process for the larger areas.

unique_stations <-
  stations %>% distinct(name, .keep_all = TRUE) %>% select(name, location)

dbWriteTable(
  conn = con,
  Id(schema = schema, table = "station_sas"),
  unique_stations,
  append =
    FALSE,
  row.names = FALSE
)

sdr_create_service_areas(
  schema = schema,
  df = unique_stations,
  identifier = "name",
  table = "station_sas",
  sa = c(1000, 5000, 10000, 20000, 30000, 40000, 60000, 80000, 105000),
  cost = "len"
)

# Create 1 minute service area - used to identify number of jobs within 1 minute
# of station
sdr_create_service_areas(
  schema = schema,
  df = unique_stations,
  identifier = "name",
  table = "station_sas",
  sa = c(1),
  cost = "time",
  target = 0.9
)

if (testing) {
  sdr_create_service_areas(
    schema = schema,
    df = unique_stations,
    identifier = "name",
    table = "station_sas",
    sa = c(5),
    cost = "time",
    target = 0.9
  )

  query <- paste0(
    "
    alter table ",
    schema,
    ".station_sas rename column service_area_5mins to service_area_60mins
    "
  )
  sdr_dbExecute(con, query)

  query <- paste0("
                  update data.stations set service_area_60mins = service_area_60mins_5mins
                  ")
  sdr_dbExecute(con, query)

} else {
  query <- paste0("
                  update data.stations set service_area_60mins = service_area_60mins_actual
                  ")
  sdr_dbExecute(con, query)

  # Create 60 minute service area - used to identify postcode centroids to be
  # considered for inclusion in model
  sdr_create_service_areas(
    schema = schema,
    df = unique_stations,
    identifier = "name",
    table = "station_sas",
    sa = c(60),
    cost = "time",
    target = 0.9
  )

}

# Add the required service area columns to proposed_stations and update
# the geometries from stations_sas.

# distance-based
for (i in c(1000, 5000, 10000, 20000, 30000, 40000, 60000, 80000, 105000)) {
  column_name <- paste0("service_area_", i / 1000, "km")
  query <-
    paste0(
      "alter table ",
      schema,
      ".proposed_stations add column if not exists ",
      column_name,
      " geometry(Polygon,27700);"
    )
  sdr_dbExecute(con, query)
  query <-
    paste0(
      "update ",
      schema,
      ".proposed_stations a set ",
      column_name,
      " = b.",
      column_name,
      " from ",
      schema,
      ".station_sas b where a.name = b.name"
    )
  sdr_dbExecute(con, query)
}

# time-based
for (i in c(1, 60)) {
  column_name <- paste0("service_area_", i, "mins")
  query <-
    paste0(
      "alter table ",
      schema,
      ".proposed_stations add column if not exists ",
      column_name,
      " geometry(Polygon,27700);"
    )
  sdr_dbExecute(con, query)
  query <-
    paste0(
      "update ",
      schema,
      ".proposed_stations a set ",
      column_name,
      " = b.",
      column_name,
      " from ",
      schema,
      ".station_sas b where a.name = b.name"
    )
  sdr_dbExecute(con, query)
}

# read sa.log into sdr.log - from workaround in foreach using sink() in
# sdr_create_service_areas

cat(read_lines(file = "sa.log"), file = "sdr.log", append = TRUE, fill = TRUE)

flog.info("station service area generation completed")


# Generate probability tables---------------------------------------------------

flog.info("starting to create choicesets and probability table(s)")

if (isolation) {
  flog.info("method is isolation")

  # which station names are duplicated?
  duplicates <- unique(stations$name[duplicated(stations$name)])

  # for crscodes where station name is NOT duplicated; straightforward
  for (crscode in stations$crscode[!(stations$name %in% duplicates)]) {
    flog.info(paste0(
      "calling sdr_generate_choicesets for: ",
      paste0(crscode, collapse = ", ")
    ))
    choicesets <- sdr_generate_choicesets(schema, crscode)

    flog.info(paste0("calling sdr_generate_probability_table for: ", crscode))
    sdr_generate_probability_table(schema, choicesets, tolower(crscode))
    choicesets <- NULL
  }

  # for duplicated stations
  # only want to get the choicesets once per duplicated station
  for (name in duplicates) {
    # get the first crscode for a station with this name
    first_crs <- stations$crscode[stations$name == name][1]

    # generate the choiceset for that crs
    flog.info(paste0("calling sdr_generate_choicesets for: ", name))
    firstsets <-
      sdr_generate_choicesets(schema, first_crs)

    # generate probability table for first_crs
    flog.info(paste0("calling sdr_generate_probability_table for: ", first_crs))
    sdr_generate_probability_table(schema, firstsets, tolower(first_crs))

    # then for every other crscode with this station name
    for (crscode in stations$crscode[stations$name == name][-1]) {
      # copy firstsets
      choicesets <- firstsets
      # update all first_crs to this crscode
      choicesets[choicesets == first_crs] <- crscode
      # create probability table
      flog.info(paste0("calling sdr_generate_probability_table for: ", crscode))
      sdr_generate_probability_table(schema, choicesets, tolower(crscode))
    }
  }

  for (crscode in stations$crscode) {
    # make frequency group adjustments if required
    if (!is.na(stations$freqgrp[stations$crscode == crscode])) {
      df <-
        data.frame(fgrp = freqgroups[freqgroups$group_id == stations$freqgrp[stations$crscode == crscode],
                                     "group_crs"], stringsAsFactors = FALSE)

      flog.info(paste0("calling sdr_frequency_group_adjustment for: ",
                       crscode))

      sdr_frequency_group_adjustment(schema, df, tolower(crscode))

    } # end if freqgrp

    # calculate the probabilities

    flog.info(paste0("calling sdr_calculate_probabilities for: ", crscode))
    sdr_calculate_probabilities(schema, tolower(crscode))
  }

  # end isolation
} else {
  # Concurrent method
  flog.info("method is concurrent")

  flog.info(paste0(
    "calling sdr_generate_choicesets for: ",
    paste0(stations$crscode, collapse = ", ")
  ))
  choicesets <- sdr_generate_choicesets(schema, stations$crscode)

  flog.info("calling sdr_generate_probability_table for concurrent")
  sdr_generate_probability_table(schema, choicesets, "concurrent")

  # make frequency group adjustments if required
  # must either be no frequency group entered or the same frequency group for
  # all stations. The latter is checked during pre-flight, so just need to take
  # the frequency group for the first stations (if it isn't NA).
  if (!is.na(stations$freqgrp[1])) {
    df <-
      data.frame(fgrp = freqgroups[freqgroups$group_id == stations$freqgrp[1], "group_crs"], stringsAsFactors = FALSE)

    flog.info("calling sdr_frequency_group_adjustment for concurrent")
    sdr_frequency_group_adjustment(schema, df, "concurrent")

  } # end if freqgrp

  flog.info("calling sdr_calculate_probabilities for concurrent")
  sdr_calculate_probabilities(schema, "concurrent")
}


# Trip end model----------------------------------------------------------------

# create and populate 1-minute workplace population column in proposed_stations

flog.info("create and populate 1-minute workplace population column in proposed_stations")
query <- paste0("
                alter table ",
                schema,
                ".proposed_stations
                add column workpop_1min int8
                ")
sdr_dbExecute(con, query)

query <- paste0(
  "
  with tmp as (
  select a.crscode, coalesce(sum(b.population), 0) as sum
  from ",
  schema,
  ".proposed_stations a
  left join data.workplace2011 b
  on st_within(b.geom, a.service_area_1mins)
  group by crscode)
  update ",
  schema,
  ".proposed_stations a
  set workpop_1min =
  (select sum from tmp where tmp.crscode = a.crscode)
  "
)
sdr_dbExecute(con, query)

# adjustments for new workplace population from exogenous input file
# update workplace pop in proposed_stations table if any of the exogenous
# centroids are within the proposed station's 1-minute service area

flog.info("make adjustments for new workplace population from exogenous_input")
query <- paste0(
  "
  with tmp as (
  select a.crscode, b.centroid, b.number
  from ",
  schema,
  ".proposed_stations a
  left join ",
  schema,
  ".exogenous_input b
  on st_within(b.geom, a.service_area_1mins)
  where b.type = 'jobs'
  )
  update ",
  schema,
  ".proposed_stations a
  set workpop_1min = workpop_1min +
  (select coalesce(sum(number), 0) from tmp where a.crscode = tmp.crscode)
  "
)
sdr_dbExecute(con, query)

# calculate probabilty weighted population for each station

# create column in proposed_stations table
query <- paste0("
                alter table ",
                schema,
                ".proposed_stations
                add column prw_pop int8
                ")
sdr_dbExecute(con, query)

for (crscode in stations$crscode) {
  if (isolation) {
    flog.info("calling sdr_calculate_prweighted_population")
    prweighted_pop <-
      sdr_calculate_prweighted_population(schema, crscode, tolower(crscode))
  } else {
    flog.info("calling sdr_calculate_prweighted_population")
    prweighted_pop <-
      sdr_calculate_prweighted_population(schema, crscode, "concurrent")

  }

  flog.info("updating proposed stations_table column: prw_pop")
  query <- paste0(
    "
    update ",
    schema,
    ".proposed_stations set prw_pop = ",
    prweighted_pop,
    " where crscode = '",
    crscode,
    "'"
  )
  sdr_dbExecute(con, query)

}


# Create GeoJSON catchment maps-------------------------------------------------


# create column in proposed_stations table

query <- paste0("
                alter table ",
                schema,
                ".proposed_stations
                add column catchment json
                ")
sdr_dbExecute(con, query)


for (crscode in stations$crscode) {
  if (isolation) {
    flog.info("calling sdr_create_json_catchment")
    sdr_create_json_catchment(schema, "proposed", crscode, tolower(crscode), tolerance = 2)
  } else {
    flog.info("calling sdr_create_json_catchment")
    sdr_create_json_catchment(schema, "proposed", crscode, "concurrent", tolerance = 2)
  }
}



# Generate forecasts------------------------------------------------------------

flog.info("generating forecasts")
query <- paste0(
  "
  alter table ",
  schema,
  ".proposed_stations
  add column forecast_base int8,
  add column forecast_uplift int8;
  "
)
sdr_dbExecute(con, query)

# version with altered application of decay function
# Coefficients:
#   Estimate Std. Error t value Pr(>|t|)
# (Intercept)                  3.601572   0.097337  37.001  < 2e-16 ***
#   log(te19cmb_15212_adj)       0.362379   0.017917  20.225  < 2e-16 ***
#   log(dailyfrequency_2013_all) 1.138605   0.027443  41.490  < 2e-16 ***
#   log1p(work_pop_1m)           0.052576   0.006840   7.687 2.48e-14 ***
#   log1p(carspaces)             0.126604   0.009169  13.808  < 2e-16 ***
#   electric_dummy               0.243148   0.040996   5.931 3.61e-09 ***
#   tcard_bound_dummy            0.294408   0.091232   3.227  0.00127 **
#   TerminusDummy                0.777482   0.083368   9.326  < 2e-16 ***
#   ---
#   Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1
#
# Residual standard error: 0.6904 on 1784 degrees of freedom
# Multiple R-squared:  0.8508,	Adjusted R-squared:  0.8502
# F-statistic:  1454 on 7 and 1784 DF,  p-value: < 2.2e-16
#

var_intercept <- 3.601572
var_population <- 0.362379
var_frequency <- 1.138605
var_workpop <- 0.052576
var_carspaces <- 0.126604
var_electric <- 0.243148
var_tcardbound <- 0.294408
var_terminus <- 0.777482

# base forecast

query <- paste0(
  "
  with tmp as (
  select id, round(exp(",
  var_intercept ,
  " + (ln(prw_pop + 1) * ",
  var_population ,
  ") + (ln(freq) * ",
  var_frequency ,
  ") + (ln(workpop_1min + 1) * ",
  var_workpop ,
  ") + (ln(carsp + 1) * ",
  var_carspaces ,
  ")
  + (electric::boolean::int * ",
  var_electric ,
  ") + (tcbound::boolean::int * ",
  var_tcardbound ,
  ") + (terminal::boolean::int * ",
  var_terminus ,
  ")))
  as forecast_base
  from ",
  schema,
  ".proposed_stations
  )
  update ",
  schema,
  ".proposed_stations a set forecast_base = tmp.forecast_base from tmp
  where a.id = tmp.id
  "
  )
sdr_dbExecute(con, query)

# regional-based uplift forecast
query <- paste0(
  "
  with tmp as (
  select a.id, round(a.forecast_base + ((b.pcchange/100) * a.forecast_base)) as forecast_uplift
  from ",
  schema,
  ".proposed_stations a
  left join data.regional_uplifts b
  on a.region = b.region
  )
  update ",
  schema,
  ".proposed_stations a set forecast_uplift = tmp.forecast_uplift from tmp
  where a.id = tmp.id
  "
)
sdr_dbExecute(con, query)


# Abstraction analysis----------------------------------------------------------

# Only process if abstraction analysis is required.
# If unique value of abstract column is > 0 (omitting NAs)

if (length(unique(na.omit(stations$abstract))) > 0) {
  flog.info("starting abstraction analysis")

  # Create the abstraction results table

  query <- paste0(
    "create table ",
    schema,
    ".abstraction_results (
    id serial primary key,
    proposed text,
    proposed_name text,
    at_risk text,
    prwpop_before int,
    prwpop_after int,
    change real,
    pc_change real,
    entsexits1718 real,
    adj_trips real,
    trips_change real,
    catchment_before json,
    catchment_after json
  )"
  )
  sdr_dbExecute(con, query)

  # populate abstraction results table with proposed (or 'concurrent') and at_risk stations
  if (isolation) {
    # for proposed stations that require an abstraction analysis
    for (proposed in stations$crscode[!is.na(stations$abstract)]) {
      # for each at-risk station specified for this proposed station
      for (at_risk in unlist(strsplit(stations$abstract[stations$crscode == proposed], ",")))
      {
        flog.info(
          paste0(
            "require result for proposed station: ",
            proposed,
            " and at risk station: ",
            at_risk
          )
        )

        query <- paste0(
          "insert into ",
          schema,
          ".abstraction_results (id, proposed, proposed_name, at_risk) values (default,'",
          proposed,
          "','",
          stations$name[stations$crscode == proposed],
          "','",
          at_risk,
          "')"
        )
        sdr_dbExecute(con, query)
      }
    }
  } else {
    # for concurrent mode at-risk stations must be the same for every station
    # so just get them from the first porposed station (if not NA)
    for (at_risk in na.omit(unlist(strsplit(stations$abstract[1], ","))))
    {
      flog.info(paste0(
        "require result for concurrent stations, and at risk station: ",
        at_risk
      ))

      query <- paste0(
        "insert into ",
        schema,
        ".abstraction_results (id, proposed, at_risk) values (default,'concurrent','",
        at_risk,
        "')"
      )
      sdr_dbExecute(con, query)
    }
  }

  # populate abstraction_results table with entsexist1718 for each at-risk station
  # How to handle this going forward?

  query <- paste0(
    "	update ",
    schema,
    ".abstraction_results a
    set entsexits1718 = b.entsexits1718
    from data.stations b
    where  a.at_risk = b.crscode"
  )
  sdr_dbExecute(con, query)


  # create the before choicesets and probability tables for each unique at-risk
  # station. Ignoring NAs of course.
  unique_atrisk <-
    unique(na.omit(unlist(strsplit(
      stations$abstract, ","
    ))))

  # Get easting and northings for these stations
  query <- paste0(
    "
    select crscode, easting || ',' || northing as location from data.stations where
    crscode in (",
    paste ("'", unique_atrisk, "'", sep = "", collapse = ",") ,
    ")
    "
    )
  unique_atrisk <- sdr_dbGetQuery(con, query)

  flog.info("starting to create BEFORE choicesets and probability tables")

  for (crscode in unique_atrisk$crscode) {
    # generate the choiceset for that crs
    flog.info(paste0("calling sdr_generate_choicesets for: ", crscode))
    choicesets <-
      sdr_generate_choicesets(schema, crscode, existing = TRUE)

    # Generate probability table
    flog.info(paste0("calling sdr_generate_probability_table for: ", crscode))
    sdr_generate_probability_table(schema, choicesets, paste0(tolower(crscode), "_before_abs"))

    # No frequency group adustments are probably needed for the before situation.
    # Is this valid? Need Confirm.

    # calculate probabilities
    flog.info(paste0("calling sdr_calculate_probabilities for: ", crscode))
    sdr_calculate_probabilities(schema, paste0(tolower(crscode),
                                               "_before_abs"))

    # get probability weighted population
    flog.info(paste0(
      "calling sdr_calculate_prweighted_population for: ",
      crscode
    ))
    prweighted_pop_before <-
      sdr_calculate_prweighted_population(schema, crscode, paste0(tolower(crscode),
                                                                  "_before_abs"))

    # update abstraction_results table for any where at_risk == crscode
    query <- paste0(
      "update ",
      schema,
      ".abstraction_results set prwpop_before = ",
      prweighted_pop_before,
      " where at_risk = '",
      crscode,
      "'"
    )
    sdr_dbExecute(con, query)
  }

  # Create the after choicesets - depends on whether isolation or concurrent method

  flog.info("starting to create AFTER choicesets and probability tables")

  # For isolation mode we loop through each proposed station that requires
  # abstraction analysis (i.e not NA)
  if (isolation) {
    for (proposed in stations$crscode[!is.na(stations$abstract)]) {
      # Then use a nested loop for each of the at-risk stations.
      for (at_risk in unlist(strsplit(stations$abstract[stations$crscode == proposed], ",")))
      {
        flog.info(
          paste0(
            "calling sdr_generate_choicesets, at risk station: ",
            at_risk,
            ", proposed station: ",
            proposed
          )
        )

        choicesets <-
          sdr_generate_choicesets(schema,
                                  proposed,
                                  existing = FALSE,
                                  abs_crs = at_risk)

        flog.info(
          paste0(
            "calling sdr_generate_probability_table, at risk station: ",
            at_risk,
            ", proposed station: ",
            proposed
          )
        )

        # Generate probability table
        sdr_generate_probability_table(schema,
                                       choicesets,
                                       paste0(tolower(at_risk),
                                              "_after_abs_",
                                              tolower(proposed)))

        # make frequency group adjustments if required
        if (!is.na(stations$freqgrp[stations$crscode == proposed])) {
          df <-
            data.frame(fgrp = freqgroups[freqgroups$group_id == stations$freqgrp[stations$crscode == proposed], "group_crs"], stringsAsFactors = FALSE)

          flog.info(
            paste0(
              "calling sdr_frequency_group_adjustment, at risk station: ",
              at_risk,
              ", proposed station: ",
              proposed
            )
          )

          sdr_frequency_group_adjustment(schema,
                                         df,
                                         paste0(tolower(at_risk),
                                                "_after_abs_",
                                                tolower(proposed)))
        } # end if freqgrp

        # calculate probabilities
        flog.info(
          paste0(
            "call sdr_calculate_probabilities, at risk station: ",
            at_risk,
            ", proposed station: ",
            proposed
          )
        )

        sdr_calculate_probabilities(schema, paste0(tolower(at_risk),
                                                   "_after_abs_",
                                                   tolower(proposed)))

        # get probability weighted population
        flog.info(
          paste0(
            "call sdr_calculate_prweighted_population, at risk station: ",
            at_risk,
            ", proposed station: ",
            proposed
          )
        )

        prweighted_pop_after <-
          sdr_calculate_prweighted_population(schema,
                                              at_risk,
                                              paste0(tolower(at_risk),
                                                     "_after_abs_",
                                                     tolower(proposed)))
        # update abstraction_results table
        query <- paste0(
          "update ",
          schema,
          ".abstraction_results
          set prwpop_after = ",
          prweighted_pop_after,
          " where proposed = '",
          proposed,
          "' and at_risk = '",
          at_risk,
          "'"
        )
        sdr_dbExecute(con, query)

        query <- paste0(
          "update ",
          schema,
          ".abstraction_results
          set change = prwpop_after - prwpop_before,
          pc_change = ((prwpop_after - prwpop_before::real) / prwpop_before) * 100
          where proposed = '",
          proposed,
          "' and at_risk = '",
          at_risk,
          "'"
        )
        sdr_dbExecute(con, query)
      }
    } # end isolation
  } else {
    # For the concurrent method, abstraction entry MUST be same for all proposed
    # stations when submitted (this is checked in pre-flight). So we just need to
    # get the entry for the first row - if not NA - and loop through each of the
    # at_risk stations.
    for (at_risk in na.omit(unlist(strsplit(stations$abstract[1], ",")))) {
      # for each at_risk station we pass all the proposed stations to the function
      # - as all would be present in the after situation in concurrent mode
      flog.info(
        paste0(
          "call sdr_generate_choicesets, at risk station: ",
          at_risk,
          ", proposed stations: ",
          paste0(stations$crscode , collapse = ", ")
        )
      )
      choicesets <-
        sdr_generate_choicesets(schema,
                                stations$crscode,
                                existing = FALSE,
                                abs_crs = at_risk)
      flog.info(
        paste0(
          "call sdr_generate_probability_table, at risk station: ",
          at_risk,
          ", proposed stations: (concurrent)"
        )
      )

      # Generate probability table
      sdr_generate_probability_table(schema,
                                     choicesets,
                                     paste0(tolower(at_risk), "_after_abs_concurrent"))


      # Make frequency group adjustments if required.
      # Must only be a single identical frequency group for all stations under
      # concurrent treatment. This is checked during pre-flight. So we just check
      # the first row for the group name and process once (if not NA)
      if (!is.na(stations$freqgrp[1])) {
        df <-
          data.frame(fgrp = freqgroups[freqgroups$group_id == stations$freqgrp[1],
                                       "group_crs"],
                     stringsAsFactors = FALSE)
        flog.info(
          paste0(
            "call sdr_frequency_group_adjustment, at risk station: ",
            at_risk,
            ", proposed stations: (concurrent)"
          )
        )

        sdr_frequency_group_adjustment(schema, df, paste0(tolower(at_risk),
                                                          "_after_abs_concurrent"))
      }

      # calculate probabilities
      flog.info(
        paste0(
          "call sdr_calculate_probabilities, at risk station: ",
          at_risk,
          ", proposed stations: (concurrent)"
        )
      )
      sdr_calculate_probabilities(schema, paste0(tolower(at_risk),
                                                 "_after_abs_concurrent"))

      # get probability weighted population

      flog.info(
        paste0(
          "call sdr_calculate_prweighted_population, at risk station: ",
          at_risk,
          ", proposed stations: (concurrent)"
        )
      )
      prweighted_pop_concurrent <-
        sdr_calculate_prweighted_population(schema,
                                            at_risk,
                                            paste0(tolower(at_risk),
                                                   "_after_abs_concurrent"))

      # update abstraction_results table
      query <- paste0(
        "update ",
        schema,
        ".abstraction_results
        set prwpop_after = ",
        prweighted_pop_concurrent,
        " where proposed = 'concurrent' and at_risk = '",
        at_risk,
        "'"
      )
      sdr_dbExecute(con, query)

      query <- paste0(
        "update ",
        schema,
        ".abstraction_results
        set change = prwpop_after - prwpop_before,
        pc_change = ((prwpop_after - prwpop_before::real) / prwpop_before) * 100
        where proposed = 'concurrent' and at_risk = '",
        at_risk,
        "'"
      )
      sdr_dbExecute(con, query)
    }
  } # end concurrent method

  # Calculate actual change and percent change between before and after situation
  query <- paste0(
    "update ",
    schema,
    ".abstraction_results
    set adj_trips = round(entsexits1718 + ((pc_change / 100) * entsexits1718))"
  )
  sdr_dbExecute(con, query)

  query <- paste0(
    "update ",
    schema,
    ".abstraction_results
    set trips_change = adj_trips - entsexits1718"
  )
  sdr_dbExecute(con, query)

  # Generate before GeoJson catchments

  # generate before catchment - only need to run for unique at-risk stations.
  # These are in unique_atrisk
  for (crscode in unique_atrisk$crscode) {
    sdr_create_json_catchment(schema,
                              "abstraction",
                              crscode,
                              paste0(tolower(crscode), "_before_abs"),
                              tolerance = 2)
  }

  # Generate after GeoJSON catchments
  if (isolation) {
    for (proposed in stations$crscode[!is.na(stations$abstract)]) {
      for (at_risk in unlist(strsplit(stations$abstract[stations$crscode == proposed], ",")))
      {
        sdr_create_json_catchment(
          schema,
          "abstraction",
          at_risk,
          paste0(tolower(at_risk),
                 "_after_abs_",
                 tolower(proposed)),
          proposed,
          tolerance = 2
        )
      }
    }
  } else {
    # concurrent
    # generate after catchment
    for (at_risk in na.omit(unlist(strsplit(stations$abstract[1], ",")))) {
      sdr_create_json_catchment(
        schema,
        "abstraction",
        at_risk,
        paste0(tolower(at_risk), "_after_abs_concurrent"),
        "concurrent",
        tolerance = 2
      )
    }
  } # end generate before and after GeoJSON catchments.

} #end abstraction analysis

# Closing actions---------------------------------------------------------------

flog.info("tidying up")

stopCluster(cl)

flog.info("model finished")

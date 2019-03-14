#Rscript main.r > output.txt

# Preliminaries-----------------------------------------------------------------

library(stationdemandr)
library(dplyr)
library(tidyr)
library(foreach)
library(stringr)
library(doParallel)
library(keyring)
library(RPostgres)
library(DBI)
library(futile.logger)
library(checkmate)

# delete any existing log files
if (file.exists("sdr.log")) {
  file.remove("sdr.log")
}

# set up logging
flog.appender(appender.file("sdr.log"))
# set logging level
flog.threshold("INFO")    # TRACE, DEBUG, INFO, WARN, ERROR, FATAL

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
testing <- TRUE

flog.info(paste0("Testing mode: ", testing))

# Set up a database connection.
# Using keyring package for storing database password in Windows credential store
# to avoid exposing on GitHub. Amend as appropriate.

con <-
  dbConnect(
    RPostgres::Postgres(),
    dbname = "dafni",
    host = "localhost",
    user = "postgres",
    password = key_get("postgres")
  )


# Set up parallel processing
# Note this is only currently used in the sdr_generate_choicesets() function in
# the foreach loop. Number of clusters is total available cores less two.


cl <- makeCluster(detectCores() - 2)
registerDoParallel(cl)

clusterEvalQ(cl, {
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
})

# Load configuration data-------------------------------------------------------


config <-
  read.csv(file = "inst/example_input/config.csv",
           sep = ";",
           stringsAsFactors = FALSE)


# check for valid mode
if (config$method == "isolation") {
  isolation <- TRUE
} else if (config$method == "concurrent") {
  isolation <- FALSE
} else {
  msg <-
    "Model method not valid, must be \"isolation\" or \"concurrent\""
  flog.fatal(msg)
  stop(msg)
}

# set schema name to job_id (check begins with a-z - required by postgresql)
if (grepl("[a-z]", substr(config$job_id, 1, 1), ignore.case = FALSE)) {
  schema <- config$job_id
} else {
  msg <-
    "database schema name uses config$job_id, first character must be lowercase a-z"
  flog.fatal(msg)
  stop(msg)
}

flog.info("config.csv has been imported and checked")



# Load station data-------------------------------------------------------------

stations <-
  read.csv(file = "inst/example_input/stations.csv",
           sep = ";",
           stringsAsFactors = FALSE)

# check id is unique

if (anyDuplicated(stations$id) > 0) {
  msg <- "station id must be unique"
  flog.fatal(msg)
  stop(msg)
}

# if concurrent
# check name is unique
# check abstraction string is same for all stations

if (isolation == FALSE) {
  if (anyDuplicated(stations$name) > 0) {
    msg <- "station name must be unique for concurrent mode"
    flog.fatal(msg)
    stop(msg)
  }
  if (length(unique(stations$abstract)) > 1) {
    msg <-
      "defined abstraction stations must be identical for all stations in concurrent mode"
    flog.fatal(msg)
    stop(msg)
  }
}

# if isolation
# check that repeated station names only differ in id, freq, freqgrp, carsp

if (isolation) {
  # if remove columns that are allowed to change
  if (nrow(stations %>% select(-id,-freq,-freqgrp,-carsp) %>%
           # distinct should only return as many rows as there are unique station names
           distinct()) != length(unique(stations$name))) {
    msg <-
      "In isolation mode stations with the same name can only differ
    in the values of id, freq, freqgrp, and carsp"
    flog.fatal(msg)
    stop(msg)
  }
}

# create location column
stations$location <-
  paste0(stations$acc_east, ",", stations$acc_north)
colnames(stations)[1] <- "crscode"


flog.info("stations.csv has been imported and checked")


# Load exogenous data-----------------------------------------------------------

exogenous <-
  read.csv(file = "inst/example_input/exogenous.csv",
           sep = ";",
           stringsAsFactors = FALSE)

# check number column can be coerced to numeric integer for all rows

if (!testInteger(exogenous$number)) {
  msg <- "exogenous$number must all be integers"
  flog.fatal(msg)
  stop(msg)
}

flog.info("exogenous.csv has been imported and checked")



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
                alter table ", schema, ".proposed_stations rename \"row_names\" TO id
                ")
sdr_dbExecute(con, query)

query <- paste0("
                alter table ", schema, ".proposed_stations alter column id type int
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

# Note: need to get Scottish workplace zone population that is now available.
# Re-calibrate the trip end model?

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

flog.info("starting to create station service areas ... ")

sdr_create_service_areas(
  schema = schema,
  df = stations,
  table = "proposed_stations",
  sa = c(1000, 5000, 10000, 20000, 30000, 40000, 60000, 80000, 105000),
  cost = "len"
)


if (testing) {
  sdr_create_service_areas(
    schema = schema,
    df = stations,
    table = "proposed_stations",
    sa = c(5),
    cost = "time",
    target = 0.9
  )

  query <- paste0(
    "
    alter table ",
    schema,
    ".proposed_stations rename column service_area_5mins to service_area_60mins
    "
  )
  sdr_dbExecute(con, query)

  query <- paste0(
    "
    update data.stations set service_area_60mins = service_area_60mins_5mins
    "
  )
  sdr_dbExecute(con, query)

} else {


  query <- paste0(
    "
    update data.stations set service_area_60mins = service_area_60mins_actual
    "
  )
  sdr_dbExecute(con, query)

  # Create 60 minute service area - used to identify postcode centroids to be
  # considered for inclusion in model


  sdr_create_service_areas(
    schema = schema,
    df = stations,
    table = "proposed_stations",
    sa = c(60),
    cost = "time",
    target = 0.9
  )

}


# Create 1 minute service area - used to identify number of jobs within 1 minute
# of station

sdr_create_service_areas(
  schema = schema,
  df = stations,
  table = "proposed_stations",
  sa = c(1),
  cost = "time",
  target = 0.9
)


flog.info("station service area generation completed")

# Generate probability tables---------------------------------------------------

flog.info("starting to generate choice sets and probability tables ...")

if (isolation) {
  # As multiple stations with the same choice set can be input for sensitivity
  # testing, we only want to generate the choice sets once for each unique station
  # as this is a long and processor intensive function. This is a bit of a
  # workaround to achieve that. May be cleaner with a re-code of various elements
  # to separate out generation of the choice sets (and the service areas above) from
  # the proposed_stations table.

  flog.info("method is isolation")

  # get df of unique station names
  unique_stations <- stations %>% distinct(name, .keep_all = FALSE)

  for (name in unique_stations$name) {

    # get the first crscode for a station with name

    first_crs <- stations$crscode[stations$name == name][1]

    # generate the choiceset for that crs

    flog.info(paste0("calling sdr_generate_choicesets for: ", name))

    station_choiceset <- sdr_generate_choicesets(schema, first_crs)

    # for every crscode with the same station
    for (crscode in stations$crscode[stations$name == name]) {

      crscode_choiceset <- station_choiceset

      # amend first_crs in the choice set to the current crscode
      crscode_choiceset[crscode_choiceset == first_crs] <- crscode

      flog.info(paste0("calling sdr_generate_probability_table for: ", crscode))

      sdr_generate_probability_table(schema, crscode_choiceset, tolower(crscode))

      # make frequency group adjustments if required
      if (!is.na(stations$freqgrp[stations$crscode == crscode])) {
        df <-
          data.frame(fgrp = config[config$group_id == stations$freqgrp[stations$crscode == crscode],
                                   "group_crs"], stringsAsFactors = FALSE)

        flog.info(paste0(
          "calling sdr_frequency_group_adjustment for: ",
          crscode
        ))

        sdr_frequency_group_adjustment(schema, df, tolower(crscode))

      } # end if freqgrp

      # calculate the probabilities

      flog.info(paste0("calling sdr_calculate_probabilities for: ", crscode))

      sdr_calculate_probabilities(schema, tolower(crscode))
    }
  }
} else {
  # Concurrent method

  flog.info("method is concurrent")

  flog.info(paste0("calling sdr_generate_choicesets for: ", paste0(stations$crscode, collapse = ", ")))

  choicesets <- sdr_generate_choicesets(schema, stations$crscode)

  flog.info("calling sdr_generate_probability_table for concurrent")

  sdr_generate_probability_table(schema, choicesets, "concurrent")

  # make frequency group adjustments if required
  # must only be a single frequency group for concurrent treatment
  # So we just check the first row for the group name and process once

  if (!is.na(stations$freqgrp[1])) {
    df <-
      data.frame(fgrp = config[config$group_id == stations$freqgrp[1], "group_crs"], stringsAsFactors = FALSE)

    flog.info("calling sdr_frequency_group_adjustment for concurrent")

    sdr_frequency_group_adjustment(schema, df, "concurrent")

  } # end if freqgrp

  flog.info("calling sdr_calculate_probabilities for concurrent")

  # calculate probabilities
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

  # update modelschema.proposed_stations table
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

flog.info("starting to generate forecasts ...")

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


# Call:
#   lm(formula = log(entex1112) ~ log(te19cmb_15212) + log(dailyfrequency_2013_all) +
#        log1p(work_pop_1m) + log1p(carspaces) + electric_dummy +
#        tcard_bound_dummy + TerminusDummy, data = catef_te_models_ews)
#
# Residuals:
#   Min      1Q  Median      3Q     Max
# -2.9479 -0.4128  0.0056  0.4287  3.3785
#
# Coefficients:
#   Estimate Std. Error t value Pr(>|t|)
# (Intercept)                  3.672122   0.095382  38.499  < 2e-16 ***
#   log(te19cmb_15212)           0.366469   0.018194  20.142  < 2e-16 ***
#   log(dailyfrequency_2013_all) 1.139167   0.027473  41.465  < 2e-16 ***
#   log1p(work_pop_1m)           0.053005   0.006840   7.749 1.54e-14 ***
#   log1p(carspaces)             0.129301   0.009147  14.136  < 2e-16 ***
#   electric_dummy               0.243414   0.041037   5.932 3.59e-09 ***
#   tcard_bound_dummy            0.299968   0.091295   3.286  0.00104 **
#   TerminusDummy                0.781959   0.083414   9.374  < 2e-16 ***
#   ---
#   Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1
#
# Residual standard error: 0.6909 on 1784 degrees of freedom
# Multiple R-squared:  0.8506,	Adjusted R-squared:   0.85
# F-statistic:  1451 on 7 and 1784 DF,  p-value: < 2.2e-16

var_intercept <- 3.672122
var_population <- 0.366469
var_frequency <- 1.139167
var_workpop <- 0.053005
var_carspaces <- 0.129301
var_electric <- 0.243414
var_tcardbound <- 0.299968
var_terminus <- 0.781959

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
# If unique value of abstract column is not a character then there are no
# abstraction stations. Depends on the input file, assumes empty in this
# position of the delimited file!

if (is.character(unique(stations$abstract))) {
  flog.info("starting abstraction analysis")

  # For before analysis we only need to consider unique crscodes (from all
  # proposed stations) where abstraction analysis is required.
  # So lets get a vector of those
  abs_stations <- unique(unlist(strsplit(stations$abstract, ",")))

  flog.info(paste0(
    "Unique crscodes requiring a BEFORE abstraction analysis: ",
    paste0(abs_stations, collapse = ", "))
  )

  # Get easting an northings for these stations
  query <- paste0(
    "
    select crscode, easting || ',' || northing as location from data.stations where
    crscode in (",
    paste ("'", abs_stations, "'", sep = "", collapse = ",") ,
    ")
    "
    )
  abs_stations <- sdr_dbGetQuery(con, query)

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
    for (proposed in stations$crscode) {
      for (at_risk in unlist(strsplit(stations$abstract[stations$crscode == proposed], ",")))
      {
        flog.info(
          paste0(
            "require result for, proposed station: ",
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
    for (at_risk in unlist(strsplit(stations$abstract[1], ",")))
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

  # populate with entsexist1718 - how to handle this going forward?

  query <- paste0(
    "	update ",
    schema,
    ".abstraction_results a
    set entsexits1718 = b.entsexits1718
    from data.stations b
    where  a.at_risk = b.crscode"
  )
  sdr_dbExecute(con, query)


  # create the before choicesets and probability tables for each unique abstraction station

  flog.info("starting to create BEFORE choicesets and probability tables ...")

  for (crscode in abs_stations$crscode) {
    # generate the choiceset for that crs

    flog.info(paste0("calling sdr_generate_choicesets for: ", crscode))

    choicesets <-
      sdr_generate_choicesets(schema, crscode, existing = TRUE)

    # Generate probability table

    flog.info(paste0("calling sdr_generate_probability_table for: ", crscode))

    sdr_generate_probability_table(schema, choicesets, paste0(tolower(crscode), "_before_abs"))

    # No frequency group adustments are probably needed for the before situation.
    # Is this valid? I think so. Confirm.

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

  flog.info("starting to create AFTER choicesets and probability tables ...")

  # For isolation we loop through each proposed station and then use a nested loop for
  # each of the stations where abstraction analysis is required

  if (isolation) {
    for (crscode in stations$crscode) {
      for (abs_crscode in unlist(strsplit(stations$abstract[stations$crscode == crscode], ",")))
      {
        flog.info(
          paste0(
            "calling sdr_generate_choicesets, at risk station: ",
            abs_crscode,
            ", proposed station: ",
            crscode
          )
        )

        choicesets <-
          sdr_generate_choicesets(schema,
                                  crscode,
                                  existing = FALSE,
                                  abs_crs = abs_crscode)

        flog.info(
          paste0(
            "calling sdr_generate_probability_table, at risk station: ",
            abs_crscode,
            ", proposed station: ",
            crscode
          )
        )

        # Generate probability table
        sdr_generate_probability_table(schema,
                                       choicesets,
                                       paste0(
                                         tolower(abs_crscode),
                                         "_after_abs_",
                                         tolower(crscode)
                                       ))

        # make frequency group adjustments if required
        if (!is.na(stations$freqgrp[stations$crscode == crscode])) {
          df <-
            data.frame(fgrp = config[config$group_id == stations$freqgrp[stations$crscode == crscode],
                                     "group_crs"],
                       stringsAsFactors = FALSE)

          flog.info(
            paste0(
              "calling sdr_frequency_group_adjustment, at risk station: ",
              abs_crscode,
              ", proposed station: ",
              crscode
            )
          )

          sdr_frequency_group_adjustment(schema,
                                         df,
                                         paste0(
                                           tolower(abs_crscode),
                                           "_after_abs_",
                                           tolower(crscode)
                                         ))

        } # end if freqgrp


        # calculate probabilities

        flog.info(
          paste0(
            "call sdr_calculate_probabilities, at risk station: ",
            abs_crscode,
            ", proposed station: ",
            crscode
          )
        )

        sdr_calculate_probabilities(schema, paste0(
          tolower(abs_crscode),
          "_after_abs_",
          tolower(crscode)
        ))


        # get probability weighted population

        flog.info(
          paste0(
            "call sdr_calculate_prweighted_population, at risk station: ",
            abs_crscode,
            ", proposed station: ",
            crscode
          )
        )

        prweighted_pop_after <-
          sdr_calculate_prweighted_population(schema,
                                              abs_crscode,
                                              paste0(
                                                tolower(abs_crscode),
                                                "_after_abs_",
                                                tolower(crscode)
                                              ))

        # update abstraction_results table

        query <- paste0(
          "update ",
          schema,
          ".abstraction_results
          set prwpop_after = ",
          prweighted_pop_after,
          " where proposed = '",
          crscode,
          "' and at_risk = '",
          abs_crscode,
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
          crscode,
          "' and at_risk = '",
          abs_crscode,
          "'"
        )
        sdr_dbExecute(con, query)

      }
    }
  } else {
    # For the concurrent method, abstraction entry MUST be same for all proposed
    # stations when submitted, so we just need to get the entry for the first row
    # and loop through each of the requested abstraction analysis stations.

    for (abs_crscode in unlist(strsplit(stations$abstract[1], ","))) {
      # for each abs_crscode we pass all the proposed stations to the function
      # as all would be present in the after situation in concurrent mode

      flog.info(
        paste0(
          "call sdr_generate_choicesets, at risk station: ",
          abs_crscode,
          ", proposed stations: ",
          paste0(stations$crscode, collapse = ", ")
        )
      )

      choicesets <-
        sdr_generate_choicesets(schema,
                                stations$crscode,
                                existing = FALSE,
                                abs_crs = abs_crscode)

      flog.info(
        paste0(
          "call sdr_generate_probability_table, at risk station: ",
          abs_crscode,
          ", proposed stations: (concurrent)"
        )
      )

      # Generate probability table
      sdr_generate_probability_table(schema,
                                     choicesets,
                                     paste0(tolower(abs_crscode), "_after_abs_concurrent"))


      # Make frequency group adjustments if required.
      # Must only be a single identical frequency group for all stations under
      # concurrent treatment. So we just check the first row for the group name
      # and process once

      if (!is.na(stations$freqgrp[1])) {
        df <-
          data.frame(fgrp = config[config$group_id == stations$freqgrp[1],
                                   "group_crs"],
                     stringsAsFactors = FALSE)

        flog.info(
          paste0(
            "call sdr_frequency_group_adjustment, at risk station: ",
            abs_crscode,
            ", proposed stations: (concurrent)"
          )
        )

        sdr_frequency_group_adjustment(schema, df, paste0(tolower(abs_crscode),
                                                          "_after_abs_concurrent"))

      }

      # calculate probabilities

      flog.info(
        paste0(
          "call sdr_calculate_probabilities, at risk station: ",
          abs_crscode,
          ", proposed stations: (concurrent)"
        )
      )

      sdr_calculate_probabilities(schema, paste0(tolower(abs_crscode),
                                                 "_after_abs_concurrent"))

      # get probability weighted population

      flog.info(
        paste0(
          "call sdr_calculate_prweighted_population, at risk station: ",
          abs_crscode,
          ", proposed stations: (concurrent)"
        )
      )

      prweighted_pop_concurrent <-
        sdr_calculate_prweighted_population(schema,
                                            abs_crscode,
                                            paste0(tolower(abs_crscode),
                                                   "_after_abs_concurrent"))

      # update abstraction_results table

      query <- paste0(
        "update ",
        schema,
        ".abstraction_results
        set prwpop_after = ",
        prweighted_pop_concurrent,
        " where proposed = 'concurrent' and at_risk = '",
        abs_crscode,
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
        abs_crscode,
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


  # Generate before and after GeoJSON catchments

  if (isolation) {
    # generate before catchment - only need to run for unique abstraction stations.
    # These are in abs_stations

    for (crscode in abs_stations$crscode) {

      flog.info("calling sdr_create_json_catchment for BEFORE catchment")

      sdr_create_json_catchment(schema,
                                "abstraction",
                                crscode,
                                paste0(tolower(crscode), "_before_abs"), tolerance = 2)
    }

    # generate after catchment

    for (crscode in stations$crscode) {
      for (atrisk_crscode in unlist(strsplit(stations$abstract[stations$crscode == crscode], ",")))
      {
        flog.info("calling sdr_create_json_catchment for AFTER catchment")

        sdr_create_json_catchment(
          schema,
          "abstraction",
          atrisk_crscode,
          paste0(
            tolower(atrisk_crscode),
            "_after_abs_",
            tolower(crscode)
          ),
          crscode, tolerance = 2
        )
      }
    }
  } else {
    # concurrent
    # generate before catchment - only need to run for unique abstraction stations.
    # These are in abs_stations

    for (crscode in abs_stations$crscode) {
      flog.info("calling sdr_create_json_catchment for BEFORE catchment")

      sdr_create_json_catchment(schema,
                                "abstraction",
                                crscode,
                                paste0(tolower(crscode), "_before_abs"), tolerance = 2)
    }

    # generate after catchment

      for (atrisk_crscode in unlist(strsplit(stations$abstract[1], ","))) {
        flog.info("calling sdr_create_json_catchment for AFTER catchment")

        sdr_create_json_catchment(
          schema,
          "abstraction",
          atrisk_crscode,
          paste0(tolower(atrisk_crscode), "_after_abs_concurrent"),
          "concurrent", tolerance = 2
        )
      }
  } # end generate before and after GeoJSON catchments.


} #end abstraction analysis


# Closing actions---------------------------------------------------------------

flog.info("tidying up")

stopCluster(cl)

flog.info("model finished")

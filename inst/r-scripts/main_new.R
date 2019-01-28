# requires: dplyr, tidyr, foreach and doParallel libraries

library(dplyr)
library(tidyr)
library(foreach)
library(parallel)
library(doParallel)

library(keyring)
library(RPostgreSQL)
drv <- dbDriver("PostgreSQL")
con <-
  dbConnect(
    drv,
    host = "localhost",
    user = "postgres",
    password = key_get("postgres"),
    dbname = "dafni"
  )

# set up parallel processing

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


# create table of exogenous inputs----------------------------------------------


dbWriteTable(
  conn = con,
  name = c(schema, 'exogenous_input'),
  exogenous,
  append =
    FALSE,
  row.names = TRUE
)

query <- paste0("
                alter table model.exogenous_input rename \"row.names\" TO id;
                ")
getQuery(con, query)

query <- paste0("
                alter table model.exogenous_input alter column id type int
                using id::integer;
                ")
getQuery(con, query)

# add and populate geom column for the exogenous centroid
# can be either a postcode centroid or a workplace centroid

# note, need to get Scottish workplace zone population that is now available.
# re-calibrate TE model?

query <- paste0("
                alter table model.exogenous_input
                add column geom geometry(Point,27700);
                ")
getQuery(con, query)

query <- paste0(
  "
  with tmp as (
  select a.centroid, coalesce(b.geom, c.geom) as geom
  from model.exogenous_input a
  left join data.pc_pop_2011 b on a.centroid = b.postcode
  left join data.workplace2011 c on a.centroid = c.wz
  )
  update model.exogenous_input a
  set geom =
  (select distinct on (centroid) geom from tmp where a.centroid = tmp.centroid)
  "
)
getQuery(con, query)


# create columns and populate data that will be used for adjusting postcode
# probability weighted population

# population column
query <- paste0("
                alter table model.exogenous_input add column population int8;
                ")
getQuery(con, query)

# average household size for the local authority where the postcode is located
query <- paste0("
                alter table model.exogenous_input add column avg_hhsize numeric;
                ")
getQuery(con, query)

# copy from number to population for type 'population'
query <- paste0("
                update model.exogenous_input set population = number where type
                ='population';
                ")
getQuery(con, query)


# for type 'houses' we get the average household size for the local authority
# in which the postcode is located then calculate population and populate
# population column and avg_hhsize column.
query <- paste0(
  "
  with tmp as (
  select a.id, c.avg_hhsize_2019, a.number * c.avg_hhsize_2019 as population
  from model.exogenous_input a
  left join data.pc_pop_2011 b
  on a.centroid = b.postcode
  left join data.hhsize c
  on b.oslaua = c.area_code
  where type = 'houses')
  update model.exogenous_input a
  set population = tmp.population,
  avg_hhsize = tmp.avg_hhsize_2019
  from tmp
  where a.id = tmp.id;
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

if (isolation) {
  # we only want to generate choice set once for each unique station
  # as this is a long and processor intense function.
  # Multiple stations with the same choice set can be input to for sensitivity
  # testing

  # This is bit of a workaround prior to a larger re-code of various elements
  # to separate out generation of the choice sets (and the service areas above)

  # get df of unique station name
  unique_stations <- stations %>% distinct(name, .keep_all = FALSE)

  for (name in unique_stations$name) {
    # get the first crscode for a station with name
    first_crs <- stations$crscode[stations$name == name][1]
    # generate the choiceset for that crs
    choicesets <- sdr_generate_choicesets(first_crs)

    # remove rows for any postcode where the proposed station is not
    # in the choice set

    choicesets <- choicesets %>%
      group_by(postcode) %>%
      filter(any(crscode == first_crs)) %>%
      ungroup


    # for every crscode with the same station
    for (crscode in stations$crscode[stations$name == name]) {
      # amend the station crscode in the choice set to the current
      # crscode
      choicesets$crscode[choicesets$crscode == first_crs] <- crscode


      # create probability table for this crscode
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

      # write the table for this crscode
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
  }
} else {
  choicesets <- sdr_generate_choicesets(stations$crscode)

  # remove rows for any postcode where none of the proposed stations are in
  # the choice set

  choicesets <- choicesets %>%
    group_by(postcode) %>%
    filter(any(crscode %in% stations$crscode)) %>%
    ungroup


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

# create and populate 1-minute workplace population column in proposed_stations

query <- paste0("
                alter table model.proposed_stations
                add column workpop_1min int8
                ")
getQuery(con, query)


query <- paste0(
  "
  with tmp as (
  select a.crscode, coalesce(sum(b.population), 0) as sum
  from model.proposed_stations a
  left join data.workplace2011 b
  on st_within(b.geom, a.service_area_1mins)
  group by crscode)
  update model.proposed_stations a
  set workpop_1min =
  (select sum from tmp where tmp.crscode = a.crscode)
  "
)
getQuery(con, query)

# adjustments for new workplace population from exogenous input file
# update workplace pop in proposed_stations table if any of the exogenous
# centroids are within the proposed station's 1-minute service area

query <- paste0(
  "
  with tmp as (
  select a.crscode, b.centroid, b.number
  from model.proposed_stations a
  left join model.exogenous_input b
  on st_within(b.geom, a.service_area_1mins)
  where b.type = 'jobs'
  )
  update model.proposed_stations a
  set workpop_1min = workpop_1min +
  (select coalesce(sum(number), 0) from tmp where a.crscode = tmp.crscode)
  "
)
getQuery(con, query)

# calculate probabilty weighted population for each station

# create column in proposed_stations table

query <- paste0("
                alter table model.proposed_stations
                add column prw_pop int8
                ")
getQuery(con, query)


for (crscode in stations$crscode) {
  if (isolation) {
    prweighted_pop <-
      sdr_calculate_prweighted_population(crscode, crscode)
  } else {
    prweighted_pop <-
      sdr_calculate_prweighted_population(crscode, "concurrent")

  }
 # update model.proposed_stations table

  query <- paste0("
                  update model.proposed_stations set prw_pop = ", prweighted_pop,
      " where crscode = '",
                  crscode, "'")
  getQuery(con, query)

}




# Generate forecasts------------------------------------------------------------

query <- paste0(
  "
  alter table model.proposed_stations
  add column forecast_base int8,
  add column forecast_uplift int8;
  "
)
getQuery(con, query)


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
  from model.proposed_stations
  )
  update model.proposed_stations a set forecast_base = tmp.forecast_base from tmp
  where a.id = tmp.id;
  "
  )
getQuery(con, query)

# regional-based uplift forecast


query <- paste0(
  "
  with tmp as (
  select a.id, round(a.forecast_base + ((b.pcchange/100) * a.forecast_base)) as forecast_uplift
  from model.proposed_stations a
  left join data.regional_uplifts b
  on a.region = b.region
  )
  update model.proposed_stations a set forecast_uplift = tmp.forecast_uplift from tmp
  where a.id = tmp.id;
  "
)
getQuery(con, query)


# Abstraction analysis----------------------------------------------------------

# only process if abstraction analysis is required.
# if unique value of abstract column is not a character then there are no abstraction stations
# care needed, depends on the input file, assumes empty in this position of delimited file.

if (!is.character(unique(test$abstract))) {
  # For some components of the analysis we only need to consider unique stations
  # where abstraction analysis is required across all the proposed stations, so
  # lets get a vector of those
  abs_stations <- unique(unlist(strsplit(stations$abstract, ",")))

  query <- paste0(
    "
    select crscode, easting || ',' || northing as location from data.stations where
    crscode in (",
    paste ("'", abs_stations, "'", sep = "", collapse = ",") ,
    ")
    "
    )
  abs_stations <- getQuery(con, query)

  # create before choicesets for each unique abstraction station

  for (crscode in abs_stations$crscode) {
    # generate the choiceset for that crs
    choicesets <-
      sdr_generate_choicesets(crscode, existing = TRUE)

    # create before probability table for this crscode
    query <- paste0(
      "create table model.probability_before_abs_",
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

    # write the table for this crscode
    dbWriteTable(
      conn = con,
      name = c('model', paste0("probability_before_abs_",
                               tolower(crscode))),
      choicesets,
      append =
        TRUE,
      row.names = FALSE
    )

    # need to populate the before tables and calulcate probabilities and get weighted population.

  }

  # create after choicesets - depends on whether isolation or concurrent method

  # for isolation we loop through each proposed station and then use a nested loop for
  # each of the stations where abstraction analysis is required

  if (isolation) {
    for (crscode in stations$crscode) {
      for (abs_crscode in unlist(strsplit(stations$abstract[stations$crscode == crscode], ",")))
      {
        choicesets <-
          sdr_generate_choicesets(crscode, existing = FALSE, abs_crs = abs_crscode)

        # remove rows for any postcode where the abs_crscode is not
        # in the choice set

        choicesets <- choicesets %>%
          group_by(postcode) %>%
          filter(any(crscode == abs_crscode)) %>%
          ungroup

        # create after probability table for this abs_crscode:crscode
        query <- paste0(
          "create table model.probability_",
          tolower(abs_crscode),
          "_after_abs_",
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

        # write the table for this abs_crscode:crscode
        dbWriteTable(
          conn = con,
          name = c('model', paste0(
            "probability_",
            tolower(abs_crscode),
            "_after_abs_",
            tolower(crscode)
          )),
          choicesets,
          append =
            TRUE,
          row.names = FALSE
        )

        # populate probability table
        sdr_populate_probability_table(paste0(
          tolower(abs_crscode),
          "_after_abs_",
          tolower(crscode)))


        # make frequency group adjustments if required
        if (!is.na(stations$freqgrp[stations$crscode == crscode])) {
          df <-
            data.frame(fgrp = config[config$group_id == stations$freqgrp[stations$crscode == crscode], "group_crs"], stringsAsFactors = FALSE)
          sdr_frequency_group_adjustment(df, paste0(
            tolower(abs_crscode),
            "_after_abs_",
            tolower(crscode)))

        } # end if freqgrp


        # calculate probabilities
        sdr_calculate_probabilities(paste0(
          tolower(abs_crscode),
          "_after_abs_",
          tolower(crscode)))


        # get probability weighted population

        prweighted_pop <-
          sdr_calculate_prweighted_population(abs_crscode, paste0(
            tolower(abs_crscode),
            "_after_abs_",
            tolower(crscode)))



      }
    }
  } else {
    # abstraction entry MUST be same for all proposed stations when submitted, so we
    # just need to get the entry for the first row and loop through each of the
    # requested abstraction analysis stations.
    for (abs_crscode in unlist(strsplit(stations$abstract[1], ","))) {

      # for each abs_crscode we pass all the proposed stations to the function
      # as all would be present in the after situation in concurrent mode
      choicesets <-
        sdr_generate_choicesets(stations$crscode,
                                         existing = FALSE,
                                         abs_crs = abs_crscode)

      # remove rows for any postcode where abs_crscode is not in
      # the choice set

      choicesets <- choicesets %>%
        group_by(postcode) %>%
        filter(any(crscode %in% abs_crscode)) %>%
        ungroup

      # create after probability table for this abs_crscode and all proposed stations
      query <- paste0(
        "create table model.probability_",
        tolower(abs_crscode),
        "_after_abs_concurrent
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

      # write the probability table for this abs_crscode and all proposed stations
      dbWriteTable(
        conn = con,
        name = c('model', paste0(
          "probability_",
          tolower(abs_crscode),
          "_after_abs_concurrent"
        )),
        choicesets,
        append =
          TRUE,
        row.names = FALSE
      )

      sdr_populate_probability_table(paste0(
        tolower(abs_crscode),
        "_after_abs_concurrent"))


      # make frequency group adjustments if required
      # must only be a single identical frequency group for all stations under concurrent treatment
      # So we just check the first row for the group name and process once

      if (!is.na(stations$freqgrp[1])) {
        df <-
          data.frame(fgrp = config[config$group_id == stations$freqgrp[1], "group_crs"], stringsAsFactors = FALSE)

        sdr_frequency_group_adjustment(df, paste0(
          tolower(abs_crscode),
          "_after_abs_concurrent"))

      }

      # calculate probabilities
      sdr_calculate_probabilities(paste0(
        tolower(abs_crscode),
        "_after_abs_concurrent"))

      # get probability weighted population

      prweighted_pop <-
        sdr_calculate_prweighted_population(abs_crscode, paste0(
          tolower(abs_crscode),
          "_after_abs_concurrent"))


    }
  }
} #end abstraction analysis

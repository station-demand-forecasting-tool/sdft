# requires: dplyr, foreach and doParallel libraries

# load configuration data

isolation <- FALSE

# load station input data-------------------------------------------------------


input <-
  read.csv(file = "inst/example_input/input.csv",
           sep = ";",
           stringsAsFactors = FALSE)


# create location column
input$location <- paste0(input$acc_east, ",", input$acc_north)
colnames(input)[1] <- "crscode"


# database setup----------------------------------------------------------------


# create db schema to hold model data

schema <- "model"

query <- paste0("
                create schema ", schema, " authorization postgres;
                ")
dbGetQuery(con, query)


# write data.frame of proposed stations to postgresql table

dbWriteTable(
  conn = con,
  name = c(schema, 'proposed_stations'),
  input,
  append =
    FALSE,
  row.names = TRUE
)

query <- paste0("
                alter table model.proposed_stations rename \"row.names\" TO id;
                ")
query <- gsub(pattern = '\\s' ,
              replacement = " ",
              x = query)
dbGetQuery(con, query)

query <- paste0("
                alter table model.proposed_stations alter column id type int
                using id::integer;
                ")
query <- gsub(pattern = '\\s' ,
              replacement = " ",
              x = query)
dbGetQuery(con, query)



# create geometry column in proposed_stations table

query <- paste0("
                alter table ",
                schema,
                ".proposed_stations add column location_geom geometry(Point,27700);
                ")
query <- gsub(pattern = '\\s' ,
              replacement = " ",
              x = query)
dbGetQuery(con, query)


# populate location_geom

query <- paste0(
  "
  update ",
  schema,
  ".proposed_stations set location_geom =
    ST_GeomFromText('POINT('||stn_east||' '||stn_north||')', 27700);
  "
)
query <- gsub(pattern = '\\s' ,
              replacement = " ",
              x = query)
dbGetQuery(con, query)


# Create station service areas--------------------------------------------------


# create distance-based service areas used in identifying nearest 10 stations to
# each postcode centroid

sdr_create_service_areas(
  df = input,
  schema = schema,
  table = "proposed_stations",
  sa = c(1000, 5000, 10000, 20000, 30000, 40000, 60000, 80000, 105000),
  cost = "len"
)

# create 60 minute service area - used to identify postcode centroids to be
# considered for inclusion in model

sdr_create_service_areas(
  df = input,
  schema = schema,
  table = "proposed_stations",
  sa = c(60),
  cost = "time",
  target = 0.9
)

# create 1 minute service area - used to identify number of jobs within 1 minute
# of station

sdr_create_service_areas(
  df = input,
  schema = schema,
  table = "proposed_stations",
  sa = c(1),
  cost = "time",
  target = 0.9
)


# create probability table------------------------------------------------------

# set up for parallel processing

cl <- makeCluster(detectCores()-2)
registerDoParallel(cl)

clusterEvalQ(cl, {
  library(DBI)
  library(RPostgreSQL)
  library(keyring)
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, host="localhost", user="postgres", password=key_get("postgres"), dbname="dafni")
  NULL
})

if (isolation) {
  for (crscode in input$crscode) {

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
    query <- gsub(pattern = '\\s' ,
                  replacement = " ",
                  x = query)
    dbGetQuery(con, query)

    dbWriteTable(
      conn = con, name = c('model', paste0(
        "probability_",
        tolower(crscode))), choicesets, append =
        TRUE, row.names = FALSE
    )

  }
} else {
  choicesets <- sdr_generate_choicesets_parallel(input$crscode)

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
  query <- gsub(pattern = '\\s' ,
                replacement = " ",
                x = query)
  dbGetQuery(con, query)

  dbWriteTable(
    conn = con, name = c('model', 'probability_concurrent'), choicesets, append =
      TRUE, row.names = FALSE
  )
}

stopCluster(cl)





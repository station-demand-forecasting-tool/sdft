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
# perhaps use unique job number from submission?

schema <- "model1"

query <- paste0("
                create schema ", schema, " authorization postgres;
                ")
dbGetQuery(con, query)


# write data.frame of stations to postgresql table

dbWriteTable(
  conn = con,
  name = c(schema, 'stations'),
  input,
  append =
    FALSE,
  row.names = FALSE
)


# create geometry column in stations table

query <- paste0("
                alter table ",
                schema,
                ".stations add column location_geom geometry(Point,27700);
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
  ".stations set location_geom =
    ST_GeomFromText('POINT('||stn_east||' '||stn_north||')', 27700);
  "
)
query <- gsub(pattern = '\\s' ,
              replacement = " ",
              x = query)
dbGetQuery(con, query)


# Create station service areas--------------------------------------------------


# create distance-based service areas used in identifying nearest 10 statiosn to
# each postcode centroid

sdr_create_service_areas(
  df = input,
  schema = schema,
  table = "stations",
  sa = c(1000, 5000, 10000, 20000, 30000, 40000, 60000, 80000, 105000),
  cost = "len"
)

# create 60 minute service area - used to identify postcode centroids to be
# considered for inclusion in model

sdr_create_service_areas(
  df = input,
  schema = schema,
  table = "stations",
  sa = c(60),
  cost = "time",
  target = 0.9
)

# create 1 minute service area - used to identify number of jobs within 1 minute
# of station

sdr_create_service_areas(
  df = input,
  schema = schema,
  table = "stations",
  sa = c(1),
  cost = "time",
  target = 0.9
)

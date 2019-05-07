# This script creates a series of distance-based service areas for each railway station
# It expects the stations table to be in a schema called 'data'.
# This will take a considerable time to complete (24 hours perhaps).
# Service areas will be used when the nearest stations to each postcode centroid need
# to be generated.


# create stations dataframe - populate with crscodes and location coordinates
query <- paste0("select crscode, longitude || ',' || latitude as location from data.stations_nrekb;")
stations <- dbGetQuery(con, query)

total_stations <- nrow(stations) # set number of records


sdr_create_service_areas(
  schema = "data",
  df = stations,
  identifier = "crscode",
  table = "stations_nrekb",
  sa = c(1000, 5000, 10000, 20000, 30000, 40000, 60000, 80000, 105000),
  cost = "len"
)

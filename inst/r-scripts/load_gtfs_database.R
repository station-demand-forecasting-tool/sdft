
library(dplyr)

stations <- read.csv("inst/source_data/gtfs/feed_26_04_2019/stops.txt", header=TRUE, stringsAsFactors=FALSE)

# Process UK rail GTFS - upload to database

# 2019

agency <- read.csv("inst/source_data/gtfs/gtfs_rail_26042019_filtered/agency.txt", header=TRUE, stringsAsFactors=FALSE)
calendar <- read.csv("inst/source_data/gtfs/gtfs_rail_26042019_filtered/calendar.txt", header=TRUE, stringsAsFactors=FALSE)
routes <- read.csv("inst/source_data/gtfs/gtfs_rail_26042019_filtered/routes.txt", header=TRUE, stringsAsFactors=FALSE)
stop_times <- read.csv("inst/source_data/gtfs/gtfs_rail_26042019_filtered/stop_times.txt", header=TRUE, stringsAsFactors=FALSE)
stops <- read.csv("inst/source_data/gtfs/gtfs_rail_26042019_filtered/stops.txt", header=TRUE, stringsAsFactors=FALSE)
transfers <- read.csv("inst/source_data/gtfs/gtfs_rail_26042019_filtered/transfers.txt", header=TRUE, stringsAsFactors=FALSE)
trips <- read.csv("inst/source_data/gtfs/gtfs_rail_26042019_filtered/trips.txt", header=TRUE, stringsAsFactors=FALSE)


# set data formats/types

calendar$start_date <- as.Date(paste(calendar$start_date), "%Y%m%d")
calendar$end_date <- as.Date(paste(calendar$end_date), "%Y%m%d")

# in stop_times delete any entries where BOTH pick_up_type and drop_off_type are 1
# these should not be in the feed - they are ignored by trip planner
# but would cause issues for example with calculating service frequency if they
# aren't remove or excluded in queries.

# Set NAs to 0 - this is fine, see: https://developers.google.com/transit/gtfs/reference/

stop_times$pickup_type[is.na(stop_times$pickup_type)] <- 0
stop_times$drop_off_type[is.na(stop_times$drop_off_type)] <- 0

stop_times <- stop_times %>% filter(!(pickup_type == 1 & drop_off_type == 1))


#2019
# Write results to a new database table
dbWriteTable(
  conn = con, name = c('gtfs2019', 'agency'), data.frame(agency), append =
    FALSE, row.names = FALSE
)


# Write results to a new database table
dbWriteTable(
  conn = con, name = c('gtfs2019', 'calendar'), data.frame(calendar), append =
    FALSE, row.names = FALSE
)

# Write results to a new database table
dbWriteTable(
  conn = con, name = c('gtfs2019', 'routes'), data.frame(routes), append =
    FALSE, row.names = FALSE
)

# Write results to a new database table
dbWriteTable(
  conn = con, name = c('gtfs2019', 'stop_times'), data.frame(stop_times), append =
    FALSE, row.names = FALSE
)


# Write results to a new database table
dbWriteTable(
  conn = con, name = c('gtfs2019', 'stops'), data.frame(stops), append =
    FALSE, row.names = FALSE
)

# Write results to a new database table
dbWriteTable(
  conn = con, name = c('gtfs2019', 'transfers'), data.frame(transfers), append =
    FALSE, row.names = FALSE
)


# Write results to a new database table
dbWriteTable(
  conn = con, name = c('gtfs2019', 'trips'), data.frame(trips), append =
    FALSE, row.names = FALSE
)

# create indexes

query <- paste(
  "create index idx_calendar_service_id
ON gtfs2019.calendar(service_id);
"
)
query <- gsub(pattern = '\\s', replacement = " ", x = query)
dbGetQuery(con, query)

query <- paste(
  "create index idx_calendar_service_id
ON gtfs2019.calendar(service_id);
"
)
query <- gsub(pattern = '\\s', replacement = " ", x = query)
dbGetQuery(con, query)
query <- paste(
  "create index idx_calendar_service_id
ON gtfs2019.calendar(service_id);
"
)
query <- gsub(pattern = '\\s', replacement = " ", x = query)
dbGetQuery(con, query)

query <- paste(
  "create index idx_calendar_service_id
ON gtfs2019.calendar(service_id);
"
)
query <- gsub(pattern = '\\s', replacement = " ", x = query)
dbGetQuery(con, query)

query <- paste(
  "create index idx_stop_times_stop_id
ON gtfs2019.stop_times(stop_id);
"
)
query <- gsub(pattern = '\\s', replacement = " ", x = query)
dbGetQuery(con, query)

query <- paste(
  "create index idx_stop_times_trip_id
ON gtfs2019.stop_times(trip_id);
"
)
query <- gsub(pattern = '\\s', replacement = " ", x = query)
dbGetQuery(con, query)

query <- paste(
  "create index idx_stops_stop_id_id
ON gtfs2019.stops(stop_id);
"
)
query <- gsub(pattern = '\\s', replacement = " ", x = query)
dbGetQuery(con, query)

query <- paste(
  "create index idx_trips_service_id
ON gtfs2019.trips(service_id);
"
)
query <- gsub(pattern = '\\s', replacement = " ", x = query)
dbGetQuery(con, query)

query <- paste(
  "create index idx_trips_trip_id
ON gtfs2019.trips(trip_id);
"
)
query <- gsub(pattern = '\\s', replacement = " ", x = query)
dbGetQuery(con, query)

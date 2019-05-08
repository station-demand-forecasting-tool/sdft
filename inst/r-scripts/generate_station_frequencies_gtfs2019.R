
# Load required libraries
library(RPostgres)

# get crscodes from  stations_nrekb

query <- paste(
  "SELECT crscode from data.stations_nrekb
"
)
query <- gsub(pattern = '\\s', replacement = " ", x = query)
crsids <- dbGetQuery(con, query)


# Note that this alternative frequency variable is for use with the Trip End models

query <- paste(
  "WITH tmp AS (
    SELECT t.*, st.* FROM gtfs2019.stop_times AS st, gtfs2019.trips AS t
    WHERE st.stop_id IN (SELECT stop_id FROM gtfs2019.stops WHERE stop_id = $1) AND st.trip_id = t.trip_id
    AND t.service_id IN (SELECT service_id FROM gtfs2019.calendar
    WHERE start_date <= '2019-05-20' AND end_date >= '2019-05-20'
    AND monday = 1)
    )
    select count(*) from tmp"
  ,
  sep = ""
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
result <-
  dbGetQuery(con, query, param = list(crsids$crscode))
crsids$freq20_05_19 <- result$count

query <- paste(
  "WITH tmp AS (
    SELECT t.*, st.* FROM gtfs2019.stop_times AS st, gtfs2019.trips AS t
    WHERE st.stop_id IN (SELECT stop_id FROM gtfs2019.stops WHERE stop_id = $1) AND st.trip_id = t.trip_id
    AND t.service_id IN (SELECT service_id FROM gtfs2019.calendar
    WHERE start_date <= '2019-06-03' AND end_date >= '2019-06-03'
    AND monday = 1)
    )
    select count(*) from tmp"
  ,
  sep = ""
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
result <-
  dbGetQuery(con, query, param = list(crsids$crscode))
crsids$freq20_06_03 <- result$count


crsids$freq <- apply(X = crsids[,2:3], MARGIN=1, FUN=max)


save(crsids, file="inst/source_data/gtfs/freq_2019.Rda")

query <- paste(
  "alter table data.stations_nrekb
        add column frequency integer"
)
query <- gsub(pattern = '\\s',replacement = " ",x = query)
dbExecute(con, query)


  query <- paste(
    "UPDATE data.stations_nrekb
        SET frequency = $1 WHERE crscode = $2", sep = ""
  )
  query <- gsub(pattern = '\\s',replacement = " ",x = query)
  dbExecute(con, query, param = list(crsids$freq, crsids$crscode))




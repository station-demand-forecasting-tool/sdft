
library(foreach)

library(doParallel)

cl <- makeCluster(6)
registerDoParallel(cl)


clusterEvalQ(cl, {
  library(DBI)
  library(RPostgreSQL)
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv, host="localhost", user="postgres", password="piazzanavona1", dbname="dafni")
  NULL
})


stopCluster(cl)

test <- foreach(i=postcodes$postcode[5000:5100], .noexport="con", .packages=c("DBI", "RPostgreSQL", "dplyr"), .combine = 'rbind') %dopar%
{

  query <- paste0(
    "
    select postcode, crscode, distance from openroads.sdr_crs_pc_nearest_stationswithpoints('",
    i,
    "', '",
    paste (crs, sep = "", collapse = ", "),
    "', 1000, 0.5)
    "
  )
  query <- gsub(pattern = '\\s' ,
                replacement = " ",
                x = query)
  nearestx <- dbGetQuery(con, query)

  if (nrow(nearestx) > 0) {

    # check for nulls. If present
    # make individual function call for each specific postcode:station pair
    # with larger bounding box.
    for (j in which(is.na(nearestx$distance))) {
      query <- paste0(
        "
        select distance from openroads.sdr_pc_station_withpoints('"
        , nearestx$postcode[j],
        "', '", nearestx$crscode[j],
        "', 25000, 1)
        "
        )
      query <- gsub(pattern = '\\s' ,
                    replacement = " ",
                    x = query)
      d <- dbGetQuery(con, query)
      # check if a distance is returned
      if (!is.null(d)) {
        nearestx$distance[j] <- d$distance
      } else {
        # if still no result set distance to -9999 for later check
        nearestx$distance[j] <- -9999
      }

    } # end for each null station

    choiceset <- nearestx %>% mutate("distance_rank" = row_number(distance)) %>%
      filter(distance_rank <= 10) %>%
      arrange(distance_rank)


  }  # end if nearestx not empty

}



stime <- system.time({
for (i in postcodes$postcode[1:1000])
{
  query <- paste0(
    "
    select postcode, crscode, distance from openroads.sdr_crs_pc_nearest_stationswithpoints('",
    i,
    "', '",
    paste (crs, sep = "", collapse = ", "),
    "', 1000, 0.5)
    "
  )
  query <- gsub(pattern = '\\s' ,
                replacement = " ",
                x = query)
  nearestx <- dbGetQuery(con, query)

  if (nrow(nearestx) > 0) {

    # check for nulls. If present
    # make individual function call for each specific postcode:station pair
    # with larger bounding box.
    for (j in which(is.na(nearestx$distance))) {
      query <- paste0(
        "
    select distance from openroads.sdr_pc_station_withpoints('"
        , nearestx$postcode[j],
        "', '", nearestx$crscode[j],
        "', 25000, 1)
    "
      )
      query <- gsub(pattern = '\\s' ,
                    replacement = " ",
                    x = query)
      d <- dbGetQuery(con, query)
      # check if a distance is returned
      if (!is.null(d)) {
        nearestx$distance[j] <- d$distance
      } else {
        # if still not set distance to -9999 for later check
        query <- paste0(
          "
          select distance from openroads.sdr_pc_station_withpoints('"
          , nearestx$postcode[j],
          "', '", nearestx$crscode[j],
          "', 25000, 1)
          "
          )
        query <- gsub(pattern = '\\s' ,
                      replacement = " ",
                      x = query)
        d <- dbGetQuery(con, query)
        nearestx$distance[j] <- -9999
      }

    } # end for each null station

      choiceset <- nearestx %>% mutate("distance_rank" = row_number(distance)) %>%
        filter(distance_rank <= 10) %>%
        arrange(distance_rank)


  }  # end if nearestx not empty
}
})



foreach(i=1:5000, .combine = 'rbind') %dopar%
{
  a <- i * 30
  a <- a * 10
  name <- paste0(i, "test")
  return(c(name, a))
}


getDoParWorkers()





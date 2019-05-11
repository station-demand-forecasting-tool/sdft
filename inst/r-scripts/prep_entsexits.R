library(dplyr)

station_usage_201718 <- read_csv("C:/Users/marcu/PhD/Data/orr/station_usage_201718.csv")

# Prepare ORR 2017/18 data

# Select columns needed

orr_usage_1718 <- station_usage_201718 %>%
  select(2:6,20)

# change colnames

colnames(orr_usage_1718) <- c("crscode", "name", "gor", "local_authority", "constituency", "entex1718")


dbWriteTable(
  conn = con,
  Id(schema = "data", table = "orr_usage_1718"),
  orr_usage_1718,
  append = FALSE,
  row.names = FALSE,
  field.types = c(entex1718 = "integer")
)


dbWriteTable(
  conn = con, name = c('data','orr_usage_1718'), data.frame(orr_usage_1718), append =
    FALSE, row.names = FALSE
)

# update data.stations with entries and exits from orr table (does TLC == CRS ??)

query <-
  paste(
    "alter table data.stations add column entsexits integer"
  )
query <- gsub(pattern = '\\s',replacement = " ",x = query)
dbExecute(con, query)

query <-
  paste(
    "UPDATE data.stations as a
    SET entsexits = b.entex1718
    FROM data.orr_usage_1718 as b
    WHERE a.crscode = b.crscode"
  )
query <- gsub(pattern = '\\s',replacement = " ",x = query)
dbExecute(con, query)

# There may be new stations with no entries/exits data yet
# Have to set these to zero.

query <-
  paste(
    "UPDATE data.stations
    SET entsexits = 0
    where entsexits is null"
  )
query <- gsub(pattern = '\\s',replacement = " ",x = query)
dbExecute(con, query)

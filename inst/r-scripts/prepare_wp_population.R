# read scottish workplace population

scottish_wp_population <- read.csv(file = "inst/source_data/workplace_zone_population_scotland.csv", stringsAsFactors = FALSE)


# write to database table

dbWriteTable(
  conn = con,
  Id(schema = "data", table = "scottish_wp_population"),
  scottish_wp_population,
  append = FALSE,
  row.names = FALSE
)

# join population to zone centroids

# set up id as primary key
query <- paste0("
                alter table data.workplace_zones_scotland
                add column population integer
                ")
sdr_dbExecute(con, query)

query <- paste0("
                update data.workplace_zones_scotland a
set population = b.population
from data.scottish_wp_population b
WHERE a.wpz = b.id
                ")
sdr_dbExecute(con, query)

# create index

query <- paste(
  "create index idx_workplace2011_geom
  ON data.workplace2011
  USING GIST (geom);
  "
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbExecute(con, query)

query <- paste(
  "create index idx_workplace2011_wz
  ON data.workplace2011(wz)
  "
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbExecute(con, query)



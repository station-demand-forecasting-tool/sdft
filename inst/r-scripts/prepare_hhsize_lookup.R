hhsize_eng <- read.csv(file = "inst/data/england_avg_hhsize_2019pred.csv", stringsAsFactors = FALSE)
hhsize_wales <- read.csv(file = "inst/data/wales_avg_hhsize_2019pred.csv", stringsAsFactors = FALSE)
hhsize_scot <- read.csv(file = "inst/data/scotland_avg_hhsize_2019pred.csv", stringsAsFactors = FALSE)

hhsize <- rbind(hhsize_eng, hhsize_wales, hhsize_scot)

dbWriteTable(
  conn = con,
  name = c('data', 'hhsize'),
  hhsize,
  append =
    FALSE,
  row.names = TRUE
)

query <- paste0("
                alter table data.hhsize rename \"row.names\" TO id;
                ")
getQuery(con, query)

query <- paste0("
                alter table data.hhsize alter column id type int
                using id::integer;
                ")
getQuery(con, query)


# Change in entries/exits by region  11/12 to 18/19 from calibration dataset


# check for NAs for gor
catef_te_models_ews %>% filter(is.na(gor))

# check for entex1819 NA
catef_te_models_ews %>% filter(is.na(entex1819))

# update PNC

catef_te_models_ews$gor[catef_te_models_ews$crscode == "PNC"] <- "Wales - Cymru"



regional_uplifts <-
  catef_te_models_ews %>%
  group_by(gor) %>%
  select(region = gor, entex1112, entex1819) %>%
  summarise(
    total1112 = sum(entex1112),
    total1819 =  sum(entex1819),
    pcchange = round(((total1819 - total1112) / total1112) * 100, 2)
  )

save(regional_uplifts, file="demand_models/predictions/regional_uplifts_1819.Rda")

# write data.frame to database table

dbWriteTable(
  conn = con,
  name = c("data", 'regional_uplifts'),
  regional_uplifts,
  append =
    FALSE,
  row.names = TRUE
)

query <- paste0("
                alter table data.regional_uplifts rename \"row.names\" TO id;
                ")
sdr_dbGetQuery(con, query)

query <- paste0("
                alter table data.regional_uplifts alter column id type int
                using id::integer;
                ")
sdr_dbGetQuery(con, query)

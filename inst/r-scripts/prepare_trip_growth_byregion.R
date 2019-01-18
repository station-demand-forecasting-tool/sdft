
# Change in entries/exits by region  11/12 to 17/18 from calibration dataset


# check for NAs for gor
catef_te_models_ews %>% filter(is.na(gor))

# check for entex1718 NA
catef_te_models_ews %>% filter(is.na(entex1718))

# update PNC

catef_te_models_ews$gor[catef_te_models_ews$crscode == "PNC"] <- "Wales - Cymru"



regional_uplifts <-
  catef_te_models_ews %>%
  group_by(gor) %>%
  select(region = gor, entex1112, entex1718) %>%
  summarise(
    total1112 = sum(entex1112),
    total1718 =  sum(entex1718),
    pcchange = round(((total1718 - total1112) / total1112) * 100, 2)
  )

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
getQuery(con, query)

query <- paste0("
                alter table data.regional_uplifts alter column id type int
                using id::integer;
                ")
getQuery(con, query)

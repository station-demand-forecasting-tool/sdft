


sdr_create_probability_table <- function(df, tablesuffix) {

  query <- paste0(
    "create table model.probability_",
    tablesuffix,
    "
        (
        id            serial primary key,
        postcode      text,
        crscode       text,
        distance      double precision,
        distance_rank smallint
        );
        "
  )
  getQuery(con, query)

  # write the table for this crscode
  dbWriteTable(
    conn = con,
    name = c('model', paste0("probability_",
                             tablesuffix)),
    df,
    append =
      TRUE,
    row.names = FALSE
  )


}

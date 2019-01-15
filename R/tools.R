# from https://stackoverflow.com/questions/44853322/how-to-read-the-contents-of-an-sql-file-into-an-r-script-to-run-a-query?noredirect=1&lq=1

# suitable for reading sql from a single file that can be submitted as a single query

getSQL <- function(filepath){
  con = file(filepath, "r")
  sql.string <- ""

  while (TRUE){
    line <- readLines(con, n = 1)

    if ( length(line) == 0 ){
      break
    }

    line <- gsub("\\t", " ", line)

    if(grepl("--",line) == TRUE){
      line <- paste(sub("--","/*",line),"*/")
    }

    sql.string <- paste(sql.string, line)
  }

  close(con)
  return(sql.string)
}


# formats db query before sending
getQuery <- function(con, query) {
  query <- gsub(pattern = '\\s' ,
                replacement = " ",
                x = query)
  dbGetQuery(con, query)
}

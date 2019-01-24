#' Reads a SQL query from a file
#'
#' This function is intended to read a long and complex SQL query from a file,
#' where it could be edited in a SQL editor, that can then be submitted as a postgreSQL query.
#'
#' This function is based on code obtained from \url{https://stackoverflow.com/questions/44853322/how-to-read-the-contents-of-an-sql-file-into-an-r-script-to-run-a-query?noredirect=1&lq=1}.
#' @param filepath The filepath of the SQL file.
#' @return A formatted sql query as a string.
#' @importFrom magrittr %>%
#' @export
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


#' Formats an RPostgreSQL query and then submits the query
#'
#'
#' This function allows the text of a SQL query to be formatted across multiple
#' lines in an R document, by stripping the line breaks prior to submitting the
#' query based on provided \code{con} object.
#'
#' @param con A database connection object for RPostgreSQL.
#' @param query The query to be formatted.
#' @export
getQuery <- function(con, query) {
  query <- gsub(pattern = '\\s' ,
                replacement = " ",
                x = query)
  RPostgreSQL::dbGetQuery(con, query)
}

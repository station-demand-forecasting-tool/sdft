queue_processor <- function() {
  library(DBI)
  library(keyring)
  library(sdft)

  con <-
    dbConnect(
      RPostgres::Postgres(),
      dbname = 'sdft',
      host = 'localhost',
      port = 5444,
      user = "postgres",
      password = keyring::key_get("postgres")
    )
  on.exit(dbDisconnect(con))


  while (TRUE) {

    tryCatch(
      {
    Sys.sleep(60)
    rs <- dbSendQuery(con,
                      "
                    SELECT job_id, method, testing, loglevel, cores
                    FROM jobs.job_queue
                    WHERE status = 0
                    ORDER BY timestamp asc
                    LIMIT 1
                      ")

    config <- dbFetch(rs)
    job_id <- config[1, 1]
    dbClearResult(rs)

    if (!is.na(job_id)) {

      dbExecute(con,
                "
                    update jobs.job_queue
                    set status = 1
                    where job_id  = $1
                      ",
                params = list(job_id))

      write.csv2(config, file = "C:/Temp/sdft/input/config.csv", row.names = FALSE, quote = FALSE)

      sdft::sdft_submit(dbhost = "localhost", dbname = 'sdft', dbport = 5444, dirpath = "C:/Temp/sdft")

      dbExecute(con,
                "
                    update jobs.job_queue
                    set status = 2
                    where job_id  = $1
                      ",
                params = list(job_id))

    }
      },
        error = function(e) {
        # Something went wrong. Report error and carry on
        writeLines(paste(Sys.time(), job_id, "Failed to run job:", e$message))
          dbExecute(con,
                    "
                    update jobs.job_queue
                    set status = 9
                    where job_id  = $1
                      ",
                    params = list(job_id))
      })

      }

}


stdout_1 <- paste0("C:/Temp/sdft/queue_log_", Sys.Date())
rp <- callr::r_bg(queue_processor, stdout = stdout_1, stderr = stdout_1)

# paste(readLines(con = stdout_1), collapse = "\n")

# rp$kill()

# check that this can run in background on Ubuntu server via RScript
# on ubuntu run this as a service daemon

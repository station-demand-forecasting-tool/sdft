worker <- function() {
  library(DBI)
  library(keyring)
  db_worker <- dbConnect(RPostgres::Postgres(), dbname = 'sdft', host = 'localhost', port = 5444, user = "postgres", password = keyring::key_get("postgres"))
  on.exit(dbDisconnect(db_worker))
  dbExecute(db_worker, "LISTEN sdft")
  dbExecute(db_worker, "LISTEN sdft_shutdown")

  while (TRUE) {
    # Wait for new work to do
    n <- RPostgres::postgresWaitForNotify(db_worker, 60)
    if (is.null(n)) {
      # If nothing to do, send notifications of any not up-to-date work
      dbExecute(db_worker, "
                SELECT pg_notify('sdft', job_id::TEXT)
                  FROM jobs.job_queue
                 WHERE status IS NULL
            ")
      next
    }

    # If we've been told to shutdown, stop right away
    if (n$channel == 'sdft_shutdown') {
      writeLines("Shutting down.")
      break
    }

    job_id <- n$payload
    tryCatch(
      {
        dbWithTransaction(db_worker, {
          # Try and fetch the item we got notified about
          rs <- dbSendQuery(db_worker, "
                    SELECT job_id
                      FROM jobs.job_queue
                     WHERE status IS NULL -- if another worker already finished, don't reprocess
                       AND job_id = $1
                       FOR UPDATE SKIP LOCKED -- Don't let another worker work on this at the same time
                ", params = list(job_id))
          job_id <- dbFetch(rs)[1, 1]
          dbClearResult(rs)

          if (!is.na(job_id)) {
            # Actually do the sqrt
            writeLines(paste("running job", job_id, "... "))
            #Sys.sleep(in_val * 0.1)
            status <- paste("done:", job_id)

            # Update the database with the result
            dbExecute(db_worker, "
                      UPDATE jobs.job_queue
                         SET status = $1
                       WHERE job_id = $2
                  ", params = list(status, job_id))
          } else {
            writeLines(paste("Not running job as another worker got there first"))
          }
        })
      },
      error = function(e) {
        # Something went wrong. Report error and carry on
        writeLines(paste("Failed to run job:", e$message))
      })
  }
}

stdout_1 <- tempfile()
rp <- callr::r_bg(worker, stdout = stdout_1, stderr = stdout_1)

# rp$kill()

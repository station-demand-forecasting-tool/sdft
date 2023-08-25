queue_processor <- function() {
  library(DBI)
  library(keyring)
  library(sdft)
  library(mailR)

  con <-
    dbConnect(
      RPostgres::Postgres(),
      dbname = 'sdft',
      host = 'localhost',
      port = 5444,
      user = "sdft",
      password = Sys.getenv('SDFT_PG_PASSWORD')
    )
  on.exit(dbDisconnect(con))


  while (TRUE) {
    tryCatch({
      Sys.sleep(60)
      rs <- dbSendQuery(
        con,
        "
                    SELECT job_id, method, testing, loglevel, cores
                    FROM jobs.job_queue
                    WHERE status = 0
                    ORDER BY timestamp asc
                    LIMIT 1
                      "
      )

      config <- dbFetch(rs)
      job_id <- config[1, 1]
      dbClearResult(rs)

      if (!is.na(job_id)) {
        # update status to running (1)

        dbExecute(con,
                  "
                    update jobs.job_queue
                    set status = 1
                    where job_id  = $1
                      ",
                  params = list(job_id))

        job_directory <- paste0("C:/Temp/sdft/jobs/", job_id)

        if (!dir.exists(job_directory)) {
          dir.create(job_directory)
          dir.create(paste0(job_directory, "/input"))
        }


        # get stations data

        rs <- dbSendQuery(con,
                          "
                    SELECT *
                    FROM jobs.job_stations
                    WHERE job_id = $1
                      ",
                          params = list(job_id))

        stations <- dbFetch(rs)
        dbClearResult(rs)

        ## need to check that we have data for stations

        # get freqgroups data

        rs <- dbSendQuery(
          con,
          "
                    SELECT group_id, group_crs
                    FROM jobs.job_freqgroups
                    WHERE job_id = $1
                      ",
          params = list(job_id)
        )

        freqgroups <- dbFetch(rs)
        dbClearResult(rs)

        # get exogenous data

        rs <- dbSendQuery(
          con,
          "
                    SELECT type, number, centroid
                    FROM jobs.job_exogenous
                    WHERE job_id = $1
                      ",
          params = list(job_id)
        )

        exogenous <- dbFetch(rs)
        dbClearResult(rs)


        write.csv2(
          config,
          file = paste0("C:/Temp/sdft/jobs/", job_id, "/input/config.csv"),
          row.names = FALSE,
          quote = FALSE
        )

        write.csv2(
          stations[, 2:19],
          file = paste0("C:/Temp/sdft/jobs/", job_id, "/input/stations.csv"),
          row.names = FALSE,
          quote = FALSE,
          na = ""
        )

        write.csv2(
          freqgroups,
          file = paste0(
            "C:/Temp/sdft/jobs/",
            job_id,
            "/input/freqgroups.csv"
          ),
          row.names = FALSE,
          quote = FALSE,
          na = ""
        )

        write.csv2(
          exogenous,
          file = paste0("C:/Temp/sdft/jobs/", job_id, "/input/exogenous.csv"),
          row.names = FALSE,
          quote = FALSE,
          na = ""
        )

        sdft::sdft_submit(
          dbhost = "localhost",
          dbuser = "sdft",
          dbname = 'sdft',
          dbport = 5444,
          dirpath = job_directory
        )

        dbExecute(con,
                  "
                    update jobs.job_queue
                    set status = 2
                    where job_id  = $1
                      ",
                  params = list(job_id))

        # send results by email

        # mailR::send.mail(from = "m.a.young@soton.ac.uk",
        #           to = c("marcus@graspit.co.uk"),
        #           #cc = c("CC Recipient <cc.recipient@gmail.com>"),
        #           #bcc = c("BCC Recipient <bcc.recipient@gmail.com>"),
        #           subject = "Subject of the email",
        #           body = "Body of the email",
        #           attach.files = c("/home/may1y17/v0.3.2.zip"),
        #           smtp = list(host.name = "smtp.soton.ac.uk", port = 25),
        #           authenticate = FALSE,
        #           send = TRUE)

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


stdout_1 <- paste0("C:/Temp/sdft/jobs/queue_log_", Sys.Date())
rp <-
  callr::r_bg(queue_processor, stdout = stdout_1, stderr = stdout_1)

# paste(readLines(con = stdout_1), collapse = "\n")

# rp$kill()

# check that this can run in background on Ubuntu server via RScript
# on ubuntu run this as a service daemon

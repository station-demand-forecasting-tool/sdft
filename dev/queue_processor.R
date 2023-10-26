#!/usr/bin/Rscript
#queue_processor <- function() {
  library(DBI)
  library(sdft)
  library(mailR)
  library(zip)

  con <-
    dbConnect(
      RPostgres::Postgres(),
      dbname = Sys.getenv('SDFT_PG_DB'),
      host = Sys.getenv('SDFT_PG_HOST'),
      port = Sys.getenv('SDFT_PG_PORT'),
      user = Sys.getenv('SDFT_PG_USER'),
      password = Sys.getenv('SDFT_PG_PASSWORD')
    )
  on.exit(dbDisconnect(con))


  while (TRUE) {
    tryCatch({
      # sleep determines how often job_queue table is queried for waiting jobs
      # by this worker if no job is running
      Sys.sleep(60)

      # we use FOR UPDATE to lock row for update
      # and SKIP LOCKED - any other worker won't select the same queued job
      # this enables use of multiple queue_processors (worker)
      # has to be within a transaction (dbBegin | dbCommit)
      # see: https://rpostgres.r-dbi.org/reference/postgres-transactions
      # and: https://spin.atomicobject.com/2021/02/04/redis-postgresql/
      dbBegin(con)
      config <- dbGetQuery(
        con,
        "
										WITH job as (
                    SELECT job_id as id
                    FROM jobs.job_queue
                    WHERE status = 0
                    ORDER BY timestamp asc
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    )
                    UPDATE jobs.job_queue
                    SET status = 1
                    FROM job
                    WHERE job_id = job.id
                    RETURNING
                     job_id, email, method, mode, loglevel, cores;
                      "
      )
      dbCommit(con)

      job_id <- config[1, 1]
      job_email <- config[1, 2]
      # discard email as not used for job config
      config$email <- NULL

      if (!is.na(job_id)) {

        job_directory <- paste0("/srv/sdft/jobs/", job_id)

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
          file = paste0("/srv/sdft/jobs/", job_id, "/input/config.csv"),
          row.names = FALSE,
          quote = FALSE
        )

        write.csv2(
          stations[, 2:19],
          file = paste0("/srv/sdft/jobs/", job_id, "/input/stations.csv"),
          row.names = FALSE,
          quote = FALSE,
          na = ""
        )

        write.csv2(
          freqgroups,
          file = paste0(
            "/srv/sdft/jobs/",
            job_id,
            "/input/freqgroups.csv"
          ),
          row.names = FALSE,
          quote = FALSE,
          na = ""
        )

        write.csv2(
          exogenous,
          file = paste0("/srv/sdft/jobs/", job_id, "/input/exogenous.csv"),
          row.names = FALSE,
          quote = FALSE,
          na = ""
        )

        sdft::sdft_submit(
          dbhost = "localhost",
          dbuser = "sdft",
          dbname = 'sdft',
          dbport = 5432,
          dirpath = job_directory
        )

        # update job status
        dbExecute(con,
                  "
                    update jobs.job_queue
                    set status = 2,
                    end_timestamp = $1
                    where job_id  = $2
                      ",
                  params = list(format(Sys.time(), "%Y-%m-%d %X"), job_id))

        # send results by email

        # first zip them

        files_to_zip <- list.files(path = paste0("/srv/sdft/jobs/", job_id, "/output/", job_id), full.names=TRUE)
        zipfile <- paste0("/srv/sdft/jobs/", job_id, "/output/", job_id, "/outputs.zip")

        # Zip the files
        zip::zipr(zipfile, files_to_zip)

        mailR::send.mail(from = "m.a.young@soton.ac.uk",
                   to = c(job_email),
                   #cc = c("CC Recipient <cc.recipient@gmail.com>"),
                   #bcc = c("BCC Recipient <bcc.recipient@gmail.com>"),
                   subject = paste0("SDFT Output for Job: ", job_id),
                   body = "/srv/sdft/job-email.html",
                   html = TRUE,
                   inline = TRUE,
                   attach.files = c(zipfile),
                   smtp = list(host.name = "smtp.soton.ac.uk", port = 25),
                   authenticate = FALSE,
                   send = TRUE)

        # tidy up
        # drop schema from database

        dbExecute(con,
                  paste0("drop schema ", job_id, " cascade")
        )

        writeLines(paste0("Job ", job_id, " completed."))

      }
    },
    error = function(e) {
      # Something went wrong. Report error and carry on
      writeLines(paste(Sys.time(), job_id, "Failed to run job:", e$message))
      dbExecute(con,
                "
                    update jobs.job_queue
                    set status = 9,
                    end_timestamp = $1
                    where job_id  = $2
                      ",
                params = list(format(Sys.time(), "%Y-%m-%d %X"), job_id))

      files_to_zip <- list.files(path = paste0("/srv/sdft/jobs/", job_id, "/output/", job_id), full.names=TRUE)
      zipfile <- paste0("/srv/sdft/jobs/", job_id, "/output/", job_id, "/outputs.zip")

      # Zip the files
      zip::zipr(zipfile, files_to_zip)

      mailR::send.mail(from = "m.a.young@soton.ac.uk",
                       to = c(job_email),
                       #cc = c("CC Recipient <cc.recipient@gmail.com>"),
                       #bcc = c("BCC Recipient <bcc.recipient@gmail.com>"),
                       subject = paste0("SDFT Job Failed: ", job_id),
                       body = "/srv/sdft/job-failed-email.html",
                       html = TRUE,
                       inline = TRUE,
                       attach.files = c(zipfile),
                       smtp = list(host.name = "smtp.soton.ac.uk", port = 25),
                       authenticate = FALSE,
                       send = TRUE)

    })

  }

#}


stdout_1 <- paste0("/srv/sdft/jobs/queue_log_", Sys.Date())
rp <-
  callr::r_bg(queue_processor, stdout = stdout_1, stderr = stdout_1)

Sys.sleep(1)

# paste(readLines(con = stdout_1), collapse = "\n")

# rp$kill()

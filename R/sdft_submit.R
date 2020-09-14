#' Main function that runs the Station Demand Forecasting Tool
#'
#' @param dbhost Character. IP address or hostname of the machine with the PostgreSQL
#' database. Default is "localhost".
#' @param dbport Integer. Port number used by the PostgreSQL server.
#' @param dbname Character. Name of the PostgreSQL database. Default is "dafni".
#' @param dbuser Character. Name of the PostgreSQL database user. Default is "postgres". Note
#' that you need to set the password for this user using the \code{key_set()} function
#' from the keyring package before running \code{sdr_submit()}.
#' @param dirpath Character. Full path to the directory containing the input folder.
#' This folder will also be used for output and must be writable. Default is the
#' current working directory. On Windows you should use forward slashes in the path.
#' @importFrom stats filter na.omit
#' @importFrom utils read.csv write.csv
#' @importFrom keyring key_get
#' @importFrom DBI dbConnect dbDriver dbWriteTable dbDisconnect
#' @importFrom parallel detectCores makeCluster clusterEvalQ clusterExport stopCluster
#' @importFrom rlang .data
#' @import doParallel
#' @import futile.logger
#' @import checkmate
#' @export

sdft_submit <-
  function(dbhost = "localhost",
           dbport = 5432,
           dbname = "dafni",
           dbuser = "postgres",
           dirpath = getwd()) {

    # Check parameters
    submit.coll <- makeAssertCollection()
    assert_character(dbhost, any.missing = FALSE, add = submit.coll)
    assert_integerish(dbport, any.missing = FALSE, add = submit.coll)
    assert_character(dbname, any.missing = FALSE, add = submit.coll)
    assert_character(dbuser, any.missing = FALSE, add = submit.coll)
    assert_character(dirpath, any.missing = FALSE, add = submit.coll)
    reportAssertions(submit.coll)

    # Configuration----------------------------------------------------------------

    # check and set directory path

    assert_directory_exists(file.path(dirpath, "input", fsep = .Platform$file.sep),
                            access = "w")

    in_path <- (file.path(dirpath, "input", fsep = .Platform$file.sep))

    # Read config file
    config <-
      read.csv(
        file = file.path(in_path, "config.csv", fsep = .Platform$file.sep),
        sep = ";",
        colClasses = c(
          "job_id" = "character",
          "method" = "character",
          "testing" = "logical",
          "loglevel" = "character",
          "cores" = "integer"
        ),
        stringsAsFactors = FALSE,
        na.strings = c("")
      )

    # Check config file

    config.coll <- makeAssertCollection()

    # Check for valid job_id.
    # Check begins with a-z, then a-z or 0-9 or _ for an additional 19 matches
    # up to max 20 characters. Valid postgreSQL schema format.
    if (isFALSE(assertTRUE(
      grepl("^[a-z][a-z0-9_]{1,19}$", config$job_id, ignore.case = FALSE),
      na.ok = FALSE,
      add = config.coll
    ))) {
      config.coll$push("The database schema name uses config$job_id, which must be in a valid
             format.")
    }

    # check for valid method

    assert_choice(config$method,
                  c("isolation", "concurrent"),
                  null.ok = FALSE,
                  add = config.coll)

    # check testing is TRUE or FALSE

    assert_logical(config$testing, null.ok = FALSE, add = config.coll)

    # check for valid logging threshold

    valid_threshold <-
      c("FATAL",
        "ERROR",
        "WARN",
        "INFO",
        "DEBUG")

    assert_choice(config$loglevel,
                  valid_threshold,
                  null.ok = FALSE,
                  add = config.coll)

    # check for valid cores - minimum of 4
    # reset to actual cores if too high.

    if (config$cores > detectCores()) {
     config$cores <- detectCores()
    }


    if (isFALSE(assert_integer(
      config$cores,
      lower = 4,
      null.ok = FALSE,
      add = config.coll
    ))) {
      config.coll$push(
        paste(
          "At least 4 cores are required. This system has",
          cores,
          " cores"
        )
      )
    }

    reportAssertions(config.coll)

    # create output directory if it doesn't already exist

    if (!dir.exists(file.path(dirpath, "output", fsep = .Platform$file.sep))) {
      dir.create(file.path(dirpath, "output", fsep = .Platform$file.sep))
    }

	# create job output folder

	out_path <-
      (file.path(dirpath, "output", config$job_id, fsep = .Platform$file.sep))

    if (!dir.exists(out_path)) {
      dir.create(out_path)
    }

    threshold <- config$loglevel


    # Setting up logging-----------------------------------------------------------

    log_file <-
      file.path(out_path, "sdr.log", fsep = .Platform$file.sep)

    # delete existing log file
    if (file.exists(log_file)) {
      file.remove(log_file)
    }

    flog.appender(appender.file(log_file))
    flog.threshold(threshold)

    # capture R errors and warnings to be logged by futile.logger
    options(
      showWarnCalls = TRUE,
      showErrorCalls = TRUE,
      show.error.locations = TRUE,
      error = function() {
        flog.error(geterrmessage())
      },
      warning.expression =
        quote({
          if (exists("last.warning", baseenv()) && !is.null(last.warning)) {
            txt <- paste0(names(last.warning), collapse = " ")
            futile.logger::flog.warn(txt)
          }
        })
    )

    flog.info("Log file initialised")

    # Setting up database connection-----------------------------------------------

    # Using keyring package for storing database password in OS credential store
    # to avoid exposing on GitHub.

    checkdb <- try(con <-
                     dbConnect(
                       RPostgres::Postgres(),
                       dbname = dbname,
                       host = dbhost,
                       port = dbport,
                       user = dbuser,
                       password = key_get(dbuser)
                     ))
    if (class(checkdb) == "try-error") {
      stop("Database connection has not been established")
    }

    # Setting up parallel processing-----------------------------------------------
    # This is currently used in the sdr_create_service_areas() and
    # sdr_generate_choicesets() functions, in a foreach loop.

    # Number of clusters from config file (reserve 2 for OS)
    cl <- makeCluster(config$cores - 2)
    registerDoParallel(cl)
    # Pass variables in this function's (sdr_main) environment to each worker in the cluster
    clusterExport(
      cl = cl,
      varlist = c("dbname", "dbport", "dbhost", "dbuser", "threshold", "out_path"),
      envir = environment()
    )

    checkcl <- try(clusterEvalQ(cl, {
      library(DBI)
      library(RPostgres)
      library(keyring)
      library(sdft)
      drv <- dbDriver("Postgres")
      con <-
        dbConnect(
          RPostgres::Postgres(),
          host = dbhost,
          port = dbport,
          user = dbuser,
          dbname = dbname,
          password = key_get(dbuser)
        )
      NULL
    }))
    if (class(checkcl) == "try-error") {
      stop("clusterEvalQ failed")
    }

    # Get station crscodes----------------------------------------------------------
    query <- paste0("
                select crscode from data.stations
                ")
    crscodes <- sdr_dbGetQuery(con, query)

    # Load input data---------------------------------------------------------------

    # check that the input files exist and can be read
    file.coll <- makeAssertCollection()
    assert_file_exists(
      file.path(in_path, "freqgroups.csv", fsep = .Platform$file.sep),
      access = "r",
      add = file.coll
    )
    assert_file_exists(
      file.path(in_path, "stations.csv", fsep = .Platform$file.sep),
      access = "r",
      add = file.coll
    )
    assert_file_exists(
      file.path(in_path, "exogenous.csv", fsep = .Platform$file.sep),
      access = "r",
      add = file.coll
    )
    reportAssertions(file.coll)


    # load freqgroups.csv
    freqgroups <-
      read.csv(
        file = file.path(in_path, "freqgroups.csv", fsep = .Platform$file.sep),
        sep = ";",
        colClasses = c("group_id" = "character",
                       "group_crs" = "character"),
        stringsAsFactors = FALSE,
        na.strings = c("")
      )

    # load stations.csv
    stations <-
      read.csv(
        file = file.path(in_path, "stations.csv", fsep = .Platform$file.sep),
        sep = ";",
        colClasses = c(
          "id" = "character",
          "stn_east" = "character",
          "stn_north" = "character",
          "acc_east" = "character",
          "acc_north" = "character",
          "freq" = "integer",
          "freqgrp" = "character",
          "carsp" = "integer",
          "ticketm" = "logical",
          "busint" = "logical",
          "cctv" = "logical",
          "terminal" = "logical",
          "electric" = "logical",
          "tcbound" = "logical",
          "category" = "character",
          "abstract" = "character"
        ),
        stringsAsFactors = FALSE,
        na.strings = c("")
      )


    # load exogenous.csv
    exogenous <-
      read.csv(
        file = file.path(in_path, "exogenous.csv", fsep = .Platform$file.sep),
        sep = ";",
        colClasses = c(
          "type" = "character",
          "number" = "integer",
          "centroid" = "character"
        ),
        stringsAsFactors = FALSE,
        na.strings = c("")
      )


    flog.info("Input files read")

    # Starting job-----------------------------------------------------------------

    # Set testing switch - If TRUE, this produces fake 60-minute proposed station
    # service areas which are actually only 5-minute service areas.
    testing <- config$testing

    # setting some variables
    have_freqgroups <- ifelse(nrow(freqgroups) > 0, TRUE, FALSE)
    have_exogenous <- ifelse(nrow(exogenous) > 0, TRUE, FALSE)
    isolation <- ifelse(config$method == "isolation", TRUE, FALSE)

    cat(
      paste0(
        "INFO [",
        format(Sys.time()),
        "] ",
        "Starting job. Logging threshold is ",
        threshold,
        "\n"
      ),
      file = file.path(log_file),
      append = TRUE
    )

    flog.info(paste0("Testing mode: ", ifelse(isTRUE(testing), "ON", "OFF")))

    # Check if the postcode_polygons table is available

    query <- paste0(
      "select exists (
   select from information_schema.tables
   where table_schema = 'data'
   and table_name = 'postcode_polygons'
)"
    )
    pcpoly <- sdr_dbGetQuery(con, query)

    # Pre-flight checks-------------------------------------------------------------

    preflight_failed <- FALSE

    # station checks---------------------------------------------------------------

    # create location column in stations (convert to numeric)
    stations$location <-
      paste0(as.numeric(stations$acc_east),
             ",",
             as.numeric(stations$acc_north))
    # rename id column
    colnames(stations)[1] <- "crscode"

    # Check that the ids (crscodes) are not used by any existing station

    idx <- which(stations$crscode %in% crscodes$crscode)

    if (length(idx) > 0) {
      preflight_failed <- TRUE
      flog.error(
        paste0(
          "The following station ids match existing station crscodes
                   and cannot be used: ",
          paste(stations$crscode[idx], collapse = ", ")
        )
      )
    }

    # check crscode is unique
    if (anyDuplicated(stations$crscode) > 0) {
      preflight_failed <- TRUE
      flog.error("Station id must be unique")
    }

    # check each row has a station name - must not be NA
    if (isFALSE(all(
      vapply(stations$name, function(x)
        grepl("^[a-zA-Z0-9 ]+$", x), logical(1), USE.NAMES = FALSE)
    ))) {
      preflight_failed <- TRUE
      flog.error("Station name must be alphanumeric string of length >= 1")
    }

    # check region is valid
    check_region <- function(region) {
      query <- paste0("select '",
                      region,
                      "' IN (select region from data.regional_uplifts)")
      as.logical(sdr_dbGetQuery(con, query))
    }

    idx <-
      which(
        vapply(stations$region, function(x)
          check_region(x), logical(1), USE.NAMES = FALSE) == FALSE
      )

    if (length(idx) > 0) {
      preflight_failed <- TRUE
      flog.error(paste0(
        "The following region(s) in station input are not valid: ",
        paste(stations$region[idx], collapse = ", ")
      ))
    }

    # station and access coordinates must be six digit strings 0-9
    if (isFALSE(all(vapply(stations$stn_east, function(x)
      grepl("^[0-9]{6}$", x), logical(1))))) {
      preflight_failed <- TRUE
      flog.error("Station eastings must be 6 character strings containing 0-9 only")
    }
    if (isFALSE(all(vapply(stations$stn_north, function(x)
      grepl("^[0-9]{6}$", x), logical(1))))) {
      preflight_failed <- TRUE
      flog.error("Station northings must be 6 character strings containing 0-9 only")
    }
    if (isFALSE(all(vapply(stations$acc_east, function(x)
      grepl("^[0-9]{6}$", x), logical(1))))) {
      preflight_failed <- TRUE
      flog.error("Access eastings must be 6 character strings containing 0-9 only")
    }
    if (isFALSE(all(vapply(stations$acc_north, function(x)
      grepl("^[0-9]{6}$", x), logical(1))))) {
      preflight_failed <- TRUE
      flog.error("Access northings must be 6 character strings containing 0-9 only")
    }

    # check if access location coordinates fall within GB extent
    check_coords <- function(coords) {
      query <- paste0(
        "
    select st_intersects(st_setsrid(st_makepoint(",
        coords,
        "),27700), geom)
    from data.gb_outline;
    "
      )
      as.logical(sdr_dbGetQuery(con, query))
    }

    idx <-
      which(
        vapply(stations$location, function(x)
          check_coords(x), logical(1), USE.NAMES = FALSE) == FALSE
      )
    if (length(idx) > 0) {
      preflight_failed <- TRUE
      flog.error(paste0(
        "The following station access points do not fall within GB: ",
        paste0(stations$crscode[idx], ": ", stations$location[idx], collapse = ", ")
      ))
    }

    # If concurrent:
    # Check station name is unique
    if (isFALSE(isolation)) {
      if (anyDuplicated(stations$name) > 0) {
        preflight_failed <- TRUE
        flog.error("Station name must be unique for concurrent mode")
      }
      # Check abstraction string is same for all stations (even if NA)
      if (length(unique(stations$abstract)) > 1) {
        preflight_failed <- TRUE
        flog.error(
          "Defined abstraction stations must be identical for all stations
               in concurrent mode"
        )
      }
      # Check same frequency group id is specified for every station (even if NA).
      if (isTRUE(have_freqgroups)) {
        if (length(unique(stations$freqgrp)) > 1) {
          preflight_failed <- TRUE
          flog.error(
            "When using concurrent mode the same frequency group must be specified
        for every station"
          )
        }
      }
    }

    # If isolation:
    # Check that any repeated station names only differ in respect of:
    # crscode, freq, freqgrp, carsp
    if (isTRUE(isolation)) {
      # if remove columns that are allowed to change
      if (nrow(
        stations %>% dplyr::select(-.data$crscode, -.data$freq, -.data$freqgrp, -.data$carsp) %>%
        # distinct should now only return as many rows as there are unique
        # station names
        dplyr::distinct()
      ) != length(unique(stations$name))) {
        preflight_failed <- TRUE
        flog.error(
          "In isolation mode stations with the same name can only differ
      in the values of id, freq, freqgrp, and carsp"
        )
      }
    }

    # check that frequency is integer > 0
    if (isFALSE(testIntegerish(stations$freq, lower = 1, any.missing = FALSE))) {
      preflight_failed <- TRUE
      flog.error("Service frequency must be an integer > 0")
    }
    # check that carsp is integer > =0
    if (isFALSE(testIntegerish(stations$carsp, lower = 0, any.missing = FALSE))) {
      preflight_failed <- TRUE
      flog.error("Parking spaces must be an integer > 0")
    }

    # check that station category is E or F
    if (isFALSE(testSubset(stations$category,
                           c("E", "F")))) {
      preflight_failed <- TRUE
      flog.error("Category must be either 'E' or 'F'")
    }

    # check abstraction station format is correct, if not NA
    # i.e. check for CSV input of three uppercase characters separated by commas
    # (also allowing single crscode with no comma). Maximum of three crscodes.
    abs_check <-
      vapply(na.omit(stations$abstract), function(x)
        grepl("^([A-Z]{3})(,[A-Z]{3}){0,2}$", x), logical(1), USE.NAMES = FALSE)
    if (isFALSE(all(abs_check))) {
      preflight_failed <- TRUE
      flog.error("Abstraction stations are not provided in the correct format")
    }

    # check that abstraction stations (if there are any) have valid crscodes
    if (length(unique(na.omit(stations$abstract))) > 0) {
      abs_crs <- unique(na.omit(unlist(strsplit(
        stations$abstract, ","
      ))))
      # get the index for those not valid
      idx <- which(!(abs_crs %in% crscodes$crscode))
      if (length(idx > 0)) {
        preflight_failed <- TRUE
        flog.error(paste(
          "The following abstraction group crscodes are not valid: ",
          paste(abs_crs[idx], collapse = ", ")
        ))
      }
    }

    # Frequency group checks---------------------------------------------------------
    if (isTRUE(have_freqgroups)) {
      # check frequency group format is ok
      fg_pairs <- unlist(strsplit(freqgroups$group_crs, ","))
      # check if all pairs match the required format
      if (isFALSE(all(
        vapply(fg_pairs, function(x)
          grepl("^[A-Z]{3}:[0-9]{1,}$", x), logical(1), USE.NAMES = FALSE)
      ))) {
        preflight_failed <- TRUE
        flog.error("Format of frequency groups is not correct")
      }

      # check crscodes in frequency groups are all valid
      # get the unique crscodes from pairs
      fg_crs <-
        unique(vapply(fg_pairs, function(x)
          sub(":.*", "", x), character(1), USE.NAMES = FALSE))
      # get the index for those not valid
      idx <- which(!(fg_crs %in% crscodes$crscode))
      if (length(idx > 0)) {
        preflight_failed <- TRUE
        flog.error(paste(
          "The following frequency group crscodes are not valid: ",
          paste(fg_crs[idx], collapse = ", ")
        ))
      }
    }

    # exogenous checks--------------------------------------------------------------
    if (isTRUE(have_exogenous)) {
      # check exogenous number column is positive integer for all rows
      if (!testIntegerish(exogenous$number,
                          lower = 1,
                          any.missing = FALSE)) {
        preflight_failed <- TRUE
        flog.error("Exogenous number values must all be positive integers")
      }

      # check valid centroids are used for the exogenous inputs
      # If type is ‘population’ or ‘housing’ the value can only be assigned to a
      # postcode centroid.
      check_pc_centroids <- function(centroid) {
        query <- paste0("select '",
                        centroid,
                        "' IN (select postcode from data.pc_pop_2011)")
        as.logical(sdr_dbGetQuery(con, query))
      }

      pop_houses <-
        exogenous %>% dplyr::filter(.data$type == "population" |
                                      .data$type == "houses")
      idx <-
        which(
          vapply(pop_houses$centroid, function(x)
            check_pc_centroids(x), logical(1), USE.NAMES = FALSE) == FALSE
        )
      if (length(idx) > 0) {
        preflight_failed <- TRUE
        flog.error(
          paste0(
            "The following postcode centroids in the exogenous input are not valid: ",
            paste0(pop_houses$type[idx], ": ", pop_houses$centroid[idx],
                   collapse = ", ")
          )
        )
      }

      # If type is ‘jobs’ the value can be assigned to either a postcode centroid or
      # a workplace centroid.
      check_wp_centroids <- function(centroid) {
        query <- paste0(
          "select '",
          centroid,
          "' IN (select wz from data.workplace2011 union select postcode from
      data.pc_pop_2011)"
        )
        as.logical(sdr_dbGetQuery(con, query))
      }

      jobs <- exogenous %>% dplyr::filter(.data$type == "jobs")
      idx <-
        which(
          vapply(jobs$centroid, function(x)
            check_wp_centroids(x), logical(1), USE.NAMES = FALSE) == FALSE
        )
      if (length(idx) > 0) {
        preflight_failed <- TRUE
        flog.error(
          paste0(
            "The following workplace centroids in the exogenous input are not valid: ",
            paste0(jobs$type[idx], ": ", jobs$centroid[idx], collapse = ", ")
          )
        )
      }
    }

    # stop if any of the pre-flight checks have failed
    if (isTRUE(preflight_failed)) {
      stop("Pre-flight checks have failed - see log file")
    } else {
      flog.info("Pre-flight checks passed")
    }


    # Database setup----------------------------------------------------------------

    # set schema name to job_id.
    schema <- config$job_id

    # create db schema to hold model data
    query <- paste0("
                create schema ", schema)
    sdr_dbExecute(con, query)

    # write data.frame of proposed stations to postgreSQL table
    dbWriteTable(
      conn = con,
      Id(schema = schema, table = "proposed_stations"),
      stations,
      append = FALSE,
      row.names = TRUE,
      field.types = c(
        ticketm = "text",
        busint = "text",
        cctv = "text",
        terminal = "text",
        electric = "text",
        tcbound = "text",
        category = "text"
      )
    )

    query <- paste0("
                alter table ",
                    schema,
                    ".proposed_stations rename \"row_names\"
                to id
                ")
    sdr_dbExecute(con, query)

    query <- paste0("
                alter table ",
                    schema,
                    ".proposed_stations alter column id type
                int using id::integer
                ")
    sdr_dbExecute(con, query)

    query <- paste0("
                alter table ",
                    schema,
                    ".proposed_stations add primary key (id)
                ")
    sdr_dbExecute(con, query)

    # create geometry column in proposed_stations table
    query <- paste0(
      "
  alter table ",
      schema,
      ".proposed_stations add column location_geom geometry(Point,27700)
  "
    )
    sdr_dbExecute(con, query)

    # populate location_geom
    query <- paste0(
      "
  update ",
      schema,
      ".proposed_stations set location_geom =
  ST_GeomFromText('POINT('||acc_east||' '||acc_north||')', 27700)
  "
    )
    sdr_dbExecute(con, query)

    flog.info("Proposed_stations table successfully created")

    # Prepare exogenous inputs------------------------------------------------------

    dbWriteTable(
      conn = con,
      Id(schema = schema, table = "exogenous_input"),
      exogenous,
      append =
        FALSE,
      row.names = TRUE,
      field.types = c(
        type = "text",
        number = "integer",
        centroid = "text"
      )
    )

    query <- paste0("
                alter table ",
                    schema,
                    ".exogenous_input rename \"row_names\" TO id
                ")
    sdr_dbExecute(con, query)

    query <- paste0("
                alter table ",
                    schema,
                    ".exogenous_input alter column id type int
                using id::integer
                ")
    sdr_dbExecute(con, query)

    query <- paste0("
                alter table ", schema, ".exogenous_input add primary key (id)
                ")
    sdr_dbExecute(con, query)

    # Add and populate geom column for the exogenous centroids.
    # Can be either a postcode centroid or a workplace centroid
    query <- paste0("
  alter table ",
                    schema,
                    ".exogenous_input add column geom geometry(Point,27700)
  ")
    sdr_dbExecute(con, query)

    query <- paste0(
      "
  with tmp as (
  select a.centroid, coalesce(b.geom, c.geom) as geom
  from ",
      schema,
      ".exogenous_input a
  left join data.pc_pop_2011 b on a.centroid = b.postcode
  left join data.workplace2011 c on a.centroid = c.wz
  )
  update ",
      schema,
      ".exogenous_input a
  set geom =
  (select distinct on (centroid) geom from tmp where a.centroid = tmp.centroid)
  "
    )
    sdr_dbExecute(con, query)

    # Create columns and populate data that will be used for adjusting postcode
    # probability weighted population

    # population column
    query <- paste0(
      "
  alter table ",
      schema,
      ".exogenous_input
  add column population int8,
  add column avg_hhsize numeric
  "
    )
    sdr_dbExecute(con, query)

    # Copy from number to population for type 'population'
    query <- paste0("
                update ",
                    schema,
                    ".exogenous_input set population = number where type
                ='population'
                ")
    sdr_dbExecute(con, query)

    # For type 'houses' we get the average household size for the local authority
    # in which the postcode is located, then calculate population and then populate
    # the population column and avg_hhsize column.
    query <- paste0(
      "
  with tmp as (
  select a.id, c.avg_hhsize_2019, a.number * c.avg_hhsize_2019 as population
  from ",
      schema,
      ".exogenous_input a
  left join data.pc_pop_2011 b
  on a.centroid = b.postcode
  left join data.hhsize c
  on b.oslaua = c.area_code
  where type = 'houses')
  update ",
      schema,
      ".exogenous_input a
  set population = tmp.population,
  avg_hhsize = tmp.avg_hhsize_2019
  from tmp
  where a.id = tmp.id
  "
    )
    sdr_dbExecute(con, query)

    flog.info("Exogenous table successfully created")


    # Create station service areas--------------------------------------------------

    # create distance-based service areas used in identifying nearest 10 stations to
    # each postcode centroid

    # Because there may be duplicate stations of the same name for sensitivity
    # analysis, we create the service areas for each unique station name in a
    # separate table and then update the proposed_stations table from that. While a
    # bit more complex, this removes unnecessary duplication of what can be a
    # relatively slow process for larger areas.

    flog.info("Starting to create station service areas")

    unique_stations <-
      stations %>%
      dplyr::distinct(.data$name, .keep_all = TRUE) %>%
      dplyr::select(.data$name, .data$location)

    dbWriteTable(
      conn = con,
      Id(schema = schema, table = "station_sas"),
      unique_stations,
      append =
        FALSE,
      row.names = FALSE
    )

    # Create service areas used during choiceset generation
    sdr_create_service_areas(
      con,
      out_path,
      schema = schema,
      df = unique_stations,
      identifier = "name",
      table = "station_sas",
      sa = c(
        1000,
        2000,
        3000,
        4000,
        5000,
        10000,
        20000,
        30000,
        40000,
        60000,
        80000,
        105000
      ),
      cost = "len"
    )

    # Create 2 minute service area - used to identify number of jobs within 2 minute
    # of station
    sdr_create_service_areas(
      con,
      out_path,
      schema = schema,
      df = unique_stations,
      identifier = "name",
      table = "station_sas",
      sa = c(2),
      cost = "time"
    )

    if (testing) {
      sdr_create_service_areas(
        con,
        out_path,
        schema = schema,
        df = unique_stations,
        identifier = "name",
        table = "station_sas",
        sa = c(5),
        cost = "time"
      )

      query <- paste0(
        "
    alter table ",
        schema,
        ".station_sas rename column service_area_5mins to service_area_60mins
    "
      )
      sdr_dbExecute(con, query)

      query <- paste0("
                  update data.stations set service_area_60mins =
                  service_area_5mins
                  ")
      sdr_dbExecute(con, query)
    } else {
      query <- paste0("
                  update data.stations set service_area_60mins =
                  service_area_60mins_actual
                  ")
      sdr_dbExecute(con, query)

      # Create 60 minute service area - used to identify postcode centroids to be
      # considered for inclusion in model
      sdr_create_service_areas(
        con,
        out_path,
        schema = schema,
        df = unique_stations,
        identifier = "name",
        table = "station_sas",
        sa = c(60),
        cost = "time",
        target = 0.9
      )
    }

    # Add the required service area columns to proposed_stations table and update
    # the geometries from stations_sas.
    # distance-based
    for (i in c(1000,
                2000,
                3000,
                4000,
                5000,
                10000,
                20000,
                30000,
                40000,
                60000,
                80000,
                105000)) {
      column_name <- paste0("service_area_", i / 1000, "km")
      query <-
        paste0(
          "alter table ",
          schema,
          ".proposed_stations add column if not exists ",
          column_name,
          " geometry(Polygon,27700);"
        )
      sdr_dbExecute(con, query)
      query <-
        paste0(
          "update ",
          schema,
          ".proposed_stations a set ",
          column_name,
          " = b.",
          column_name,
          " from ",
          schema,
          ".station_sas b where a.name = b.name"
        )
      sdr_dbExecute(con, query)
    }

    # time-based
    for (i in c(2, 60)) {
      column_name <- paste0("service_area_", i, "mins")
      query <-
        paste0(
          "alter table ",
          schema,
          ".proposed_stations add column if not exists ",
          column_name,
          " geometry(Polygon,27700);"
        )
      sdr_dbExecute(con, query)
      query <-
        paste0(
          "update ",
          schema,
          ".proposed_stations a set ",
          column_name,
          " = b.",
          column_name,
          " from ",
          schema,
          ".station_sas b where a.name = b.name"
        )
      sdr_dbExecute(con, query)
    }

    flog.info("Station service area generation completed")


    # Generate probability tables---------------------------------------------------

    flog.info("Starting to create choicesets and probability table(s)")

    if (isolation) {
      flog.info("Method is isolation")

      # which station names are duplicated?
      duplicates <- unique(stations$name[duplicated(stations$name)])

      # for crscodes where station name is NOT duplicated; straightforward
      for (crscode in stations$crscode[!(stations$name %in% duplicates)]) {
        choicesets <- sdr_generate_choicesets(con, schema, crscode)
        sdr_generate_probability_table(con, schema, choicesets, tolower(crscode))
        choicesets <- NULL
      }

      # for duplicated stations
      # only want to get the choicesets once per duplicated station
      for (name in duplicates) {
        # get the first crscode for a station with this name
        first_crs <- stations$crscode[stations$name == name][1]

        # generate the choiceset for that crs
        firstsets <-
          sdr_generate_choicesets(con, schema, first_crs)

        # generate probability table for first_crs
        sdr_generate_probability_table(con, schema, firstsets, tolower(first_crs))

        # then for every other crscode with this station name
        for (crscode in stations$crscode[stations$name == name][-1]) {
          # copy firstsets
          choicesets <- firstsets
          # update all occurrences of first_crs to this crscode
          choicesets$crscode[choicesets$crscode == first_crs] <- crscode
          # create probability table
          sdr_generate_probability_table(con, schema, choicesets, tolower(crscode))
        }
      }

      for (crscode in stations$crscode) {
        # make frequency group adjustments if required
        if (isTRUE(have_freqgroups) &
            (!is.na(stations$freqgrp[stations$crscode == crscode]))) {
          df <-
            data.frame(fgrp = freqgroups[freqgroups$group_id ==
                                           stations$freqgrp[stations$crscode == crscode],
                                         "group_crs"], stringsAsFactors = FALSE)
          sdr_frequency_group_adjustment(con, schema, df, tolower(crscode))
        }
        # calculate the probabilities
        sdr_calculate_probabilities(con, schema, tolower(crscode))
      } # end isolation
    } else {
      # Concurrent method
      flog.info("Method is concurrent")
      choicesets <-
        sdr_generate_choicesets(con, schema, stations$crscode)
      sdr_generate_probability_table(con, schema, choicesets, "concurrent")

      # make frequency group adjustments if required
      # must either be no frequency group entered or the same frequency group for
      # all stations. The latter is checked during pre-flight, so just need to take
      # the frequency group for the first station (if it isn't NA).
      if (isTRUE(have_freqgroups) & (!is.na(stations$freqgrp[1]))) {
        df <-
          data.frame(fgrp = freqgroups[freqgroups$group_id == stations$freqgrp[1],
                                       "group_crs"], stringsAsFactors = FALSE)
        sdr_frequency_group_adjustment(con, schema, df, "concurrent")
      }
      sdr_calculate_probabilities(con, schema, "concurrent")
    } # end concurrent


    # Trip end model----------------------------------------------------------------

    # create and populate 2-minute workplace population column in proposed_stations
    query <- paste0("
                alter table ",
                    schema,
                    ".proposed_stations
                add column workpop_2min int8
                ")
    sdr_dbExecute(con, query)

    query <- paste0(
      "
  with tmp as (
  select a.crscode, coalesce(sum(b.population), 0) as sum
  from ",
      schema,
      ".proposed_stations a
  left join data.workplace2011 b
  on st_within(b.geom, a.service_area_2mins)
  group by crscode)
  update ",
      schema,
      ".proposed_stations a
  set workpop_2min =
  (select sum from tmp where tmp.crscode = a.crscode)
  "
    )
    sdr_dbExecute(con, query)

    # adjustments for proposed workplace population from exogenous input file
    # update workplace pop in proposed_stations table if any of the exogenous
    # centroids are within the proposed station's 2-minute service area

    if (isTRUE(have_exogenous)) {
      flog.info("Making adjustments to workplace population from exogenous_input")
      query <- paste0(
        "
  with tmp as (
  select a.crscode, b.centroid, b.number
  from ",
        schema,
        ".proposed_stations a
  left join ",
        schema,
        ".exogenous_input b
  on st_within(b.geom, a.service_area_2mins)
  where b.type = 'jobs'
  )
  update ",
        schema,
        ".proposed_stations a
  set workpop_2min = workpop_2min +
  (select coalesce(sum(number), 0) from tmp where a.crscode = tmp.crscode)
  "
      )
      sdr_dbExecute(con, query)
    }

    # calculate probabilty weighted population for each station
    # create column in proposed_stations table
    query <- paste0("
                alter table ",
                    schema,
                    ".proposed_stations
                add column prw_pop int8
                ")
    sdr_dbExecute(con, query)

    for (crscode in stations$crscode) {
      if (isolation) {
        prweighted_pop <-
          sdr_calculate_prweighted_population(con, schema, crscode, tolower(crscode))
      } else {
        prweighted_pop <-
          sdr_calculate_prweighted_population(con, schema, crscode, "concurrent")
      }

      query <- paste0(
        "
    update ",
        schema,
        ".proposed_stations set prw_pop = ",
        prweighted_pop,
        " where crscode = '",
        crscode,
        "'"
      )
      sdr_dbExecute(con, query)
    }


    # Create GeoJSON catchment maps-------------------------------------------------
    # create column in proposed_stations table
    query <- paste0("
                alter table ",
                    schema,
                    ".proposed_stations
                add column catchment json
                ")
    sdr_dbExecute(con, query)

    for (crscode in stations$crscode) {
      if (isolation) {
        sdr_create_json_catchment(con,
                                  schema,
                                  "proposed",
                                  pcpoly$exists,
                                  crscode,
                                  tolower(crscode),
                                  tolerance = 2)
      } else {
        sdr_create_json_catchment(con,
                                  schema,
                                  "proposed",
                                  pcpoly$exists,
                                  crscode,
                                  "concurrent",
                                  tolerance = 2)
      }
    }


    # Generate forecasts------------------------------------------------------------

    flog.info("Generating demand forecasts")
    query <- paste0(
      "
  alter table ",
      schema,
      ".proposed_stations
  add column forecast_base int8,
  add column forecast_uplift int8;
  "
    )
    sdr_dbExecute(con, query)

    # version with altered application of decay function
    # and 2 minute work population (1 mile)
    # Coefficients:
    #   Estimate Std. Error t value Pr(>|t|)
    # (Intercept)                  3.515281   0.094543  37.182  < 2e-16 ***
    #   log(te19cmb_15212_adj)       0.366328   0.018018  20.331  < 2e-16 ***
    #   log(dailyfrequency_2013_all) 1.124473   0.027686  40.615  < 2e-16 ***
    #   log1p(work_pop_2m)           0.056579   0.007915   7.149 1.27e-12 ***
    #   log1p(carspaces)             0.126181   0.009195  13.723  < 2e-16 ***
    #   electric_dummy               0.236574   0.041156   5.748 1.06e-08 ***
    #   tcard_bound_dummy            0.299954   0.091444   3.280  0.00106 **
    #   TerminusDummy                0.792108   0.083448   9.492  < 2e-16 ***
    #   ---
    #   Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1
    #
    # Residual standard error: 0.6919 on 1784 degrees of freedom
    # Multiple R-squared:  0.8502,	Adjusted R-squared:  0.8496
    # F-statistic:  1446 on 7 and 1784 DF,  p-value: < 2.2e-16
    #
    var_intercept <- 3.515281
    var_population <- 0.366328
    var_frequency <- 1.124473
    var_workpop <- 0.056579
    var_carspaces <- 0.126181
    var_electric <- 0.236574
    var_tcardbound <- 0.299954
    var_terminus <- 0.792108

    # base forecast
    query <- paste0(
      "
  with tmp as (
  select id, round(exp(",
      var_intercept,
      " + (ln(prw_pop + 1) * ",
      var_population,
      ") + (ln(freq) * ",
      var_frequency,
      ") + (ln(workpop_2min + 1) * ",
      var_workpop,
      ") + (ln(carsp + 1) * ",
      var_carspaces,
      ")
  + (electric::boolean::int * ",
      var_electric,
      ") + (tcbound::boolean::int * ",
      var_tcardbound,
      ") + (terminal::boolean::int * ",
      var_terminus,
      ")))
  as forecast_base
  from ",
      schema,
      ".proposed_stations
  )
  update ",
      schema,
      ".proposed_stations a set forecast_base = tmp.forecast_base from tmp
  where a.id = tmp.id
  "
    )
    sdr_dbExecute(con, query)

    # regional-based uplift forecast
    query <- paste0(
      "
  with tmp as (
  select a.id, round(a.forecast_base + ((b.pcchange/100) * a.forecast_base))
  as forecast_uplift from ",
      schema,
      ".proposed_stations a
  left join data.regional_uplifts b
  on a.region = b.region
  )
  update ",
      schema,
      ".proposed_stations a set forecast_uplift = tmp.forecast_uplift from tmp
  where a.id = tmp.id
  "
    )
    sdr_dbExecute(con, query)

    # Abstraction analysis----------------------------------------------------------

    # Only process if abstraction analysis is required.
    if (length(unique(na.omit(stations$abstract))) > 0) {
      flog.info("Starting abstraction analysis")
      # Create the abstraction results table
      query <- paste0(
        "create table ",
        schema,
        ".abstraction_results (
    id serial primary key,
    proposed text,
    proposed_name text,
    at_risk text,
    prwpop_before int,
    prwpop_after int,
    change real,
    pc_change real,
    entsexits integer,
    adj_trips integer,
    trips_change integer,
    catchment_before json,
    catchment_after json
  )"
      )
      sdr_dbExecute(con, query)

      # populate abstraction results table with proposed (or 'concurrent') and
      # at_risk stations
      if (isTRUE(isolation)) {
        # for proposed stations that require an abstraction analysis
        for (proposed in stations$crscode[!is.na(stations$abstract)]) {
          # for each at-risk station specified for this proposed station
          for (at_risk in unlist(strsplit(stations$abstract[stations$crscode ==
                                                            proposed], ",")))
          {
            query <- paste0(
              "insert into ",
              schema,
              ".abstraction_results (id, proposed, proposed_name, at_risk)
          values (default,'",
              proposed,
              "','",
              stations$name[stations$crscode == proposed],
              "','",
              at_risk,
              "')"
            )
            sdr_dbExecute(con, query)
          }
        }
      } else {
        # for concurrent mode, at-risk stations must be the same for every station
        # so just get them from the first porposed station (if not NA)
        for (at_risk in na.omit(unlist(strsplit(stations$abstract[1], ","))))
        {
          query <- paste0(
            "insert into ",
            schema,
            ".abstraction_results (id, proposed, at_risk) values (default,'concurrent','",
            at_risk,
            "')"
          )
          sdr_dbExecute(con, query)
        }
      }

      # populate abstraction_results table with entsexits for each at-risk station
      query <- paste0(
        "	update ",
        schema,
        ".abstraction_results a
    set entsexits = b.entsexits
    from data.stations b
    where a.at_risk = b.crscode"
      )
      sdr_dbExecute(con, query)

      # create BEFORE probability tables--------------------------------------------

      # Get unique at_risk stations first
      unique_atrisk <-
        unique(na.omit(unlist(strsplit(
          stations$abstract, ","
        ))))

      # Get easting and northings for these stations
      query <- paste0(
        "
    select crscode, easting || ',' || northing as location from data.stations where
    crscode in (",
        paste(
          "'",
          unique_atrisk,
          "'",
          sep = "",
          collapse = ","
        ),
        ")
    "
      )
      unique_atrisk <- sdr_dbGetQuery(con, query)

      flog.info("Creating BEFORE choicesets and probability tables")
      for (at_risk in unique_atrisk$crscode) {
        # generate the choiceset for this at_risk station
        # only want to do this once for each at_risk.
        choicesets <-
          sdr_generate_choicesets(con, schema, at_risk, existing = TRUE)
        # Isolation mode
        if (isTRUE(isolation)) {
          # for each proposed station
          for (crscode in stations$crscode) {
            # check if this at_risk is in stations$abstract for this crscode
            if (grepl(paste(at_risk), stations$abstract[stations$crscode == crscode])) {
              # Generate probability table
              sdr_generate_probability_table(con,
                                             schema,
                                             choicesets,
                                             paste0(
                                               tolower(at_risk),
                                               "_before_abs_",
                                               tolower(crscode)
                                             ))
              # Make frequency group adjustments
              if (isTRUE(have_freqgroups) &
                  !is.na(stations$freqgrp[stations$crscode == crscode])) {
                df <-
                  data.frame(fgrp = freqgroups[freqgroups$group_id
                                               == stations$freqgrp[stations$crscode == crscode],
                                               "group_crs"], stringsAsFactors = FALSE)
                sdr_frequency_group_adjustment(con,
                                               schema,
                                               df,
                                               paste0(
                                                 tolower(at_risk),
                                                 "_before_abs_",
                                                 tolower(crscode)
                                               ))
              }
              # calculate probabilities
              sdr_calculate_probabilities(con,
                                          schema,
                                          paste0(
                                            tolower(at_risk),
                                            "_before_abs_",
                                            tolower(crscode)
                                          ))
              # get probability weighted population
              prweighted_pop_before <-
                sdr_calculate_prweighted_population(con,
                                                    schema,
                                                    at_risk,
                                                    paste0(
                                                      tolower(at_risk),
                                                      "_before_abs_",
                                                      tolower(crscode)
                                                    ))
              # update abstraction_results table
              query <- paste0(
                "update ",
                schema,
                ".abstraction_results set prwpop_before = ",
                prweighted_pop_before,
                " where at_risk = '",
                at_risk,
                "' and proposed = '",
                crscode,
                "'"
              )
              sdr_dbExecute(con, query)
              # generate GeoJSON catchments
              sdr_create_json_catchment(
                con,
                schema,
                "abstraction",
                pcpoly$exists,
                at_risk,
                paste0(
                  tolower(at_risk),
                  "_before_abs_",
                  tolower(crscode)
                ),
                tolerance = 2
              )
            } # end at_risk is in stations$abstract for this crscode
          } # end crscode in stations
        } else {
          # Now for concurrent (we are still in the unique at_risk loop)
          # Generate probability table
          sdr_generate_probability_table(con,
                                         schema,
                                         choicesets,
                                         paste0(tolower(at_risk),
                                                "_before_abs_concurrent"))
          # Make frequency group adjustments
          # Only need to look at first row as must be same for all stations
          if (isTRUE(have_freqgroups) & (!is.na(stations$freqgrp[1]))) {
            df <-
              data.frame(fgrp = freqgroups[freqgroups$group_id ==
                                             stations$freqgrp[1], "group_crs"],
                         stringsAsFactors = FALSE)
            sdr_frequency_group_adjustment(con,
                                           schema,
                                           df,
                                           paste0(tolower(at_risk),
                                                  "_before_abs_concurrent"))
          }
          # calculate probabilities
          sdr_calculate_probabilities(con,
                                      schema,
                                      paste0(tolower(at_risk),
                                             "_before_abs_concurrent"))
          # get probability weighted population
          prweighted_pop_before <-
            sdr_calculate_prweighted_population(con,
                                                schema,
                                                at_risk,
                                                paste0(tolower(at_risk),
                                                       "_before_abs_concurrent"))

          # update abstraction_results table
          query <- paste0(
            "update ",
            schema,
            ".abstraction_results set prwpop_before = ",
            prweighted_pop_before,
            " where at_risk = '",
            at_risk,
            "' and proposed = 'concurrent'"
          )
          sdr_dbExecute(con, query)
          # generate before GeoJSON catchments
          sdr_create_json_catchment(
            con,
            schema,
            "abstraction",
            pcpoly$exists,
            at_risk,
            paste0(tolower(at_risk), "_before_abs_concurrent"),
            tolerance = 2
          )
        } # end concurrent
      } # end for each unique at_risk station

      # Create AFTER probability tables---------------------------------------------
      flog.info("Creating AFTER choicesets and probability tables")
      # For isolation mode we loop through each proposed station that requires
      # abstraction analysis (i.e not NA)
      if (isTRUE(isolation)) {
        for (proposed in stations$crscode[!is.na(stations$abstract)]) {
          # Then use a nested loop for each of the at-risk stations.
          for (at_risk in unlist(strsplit(stations$abstract[stations$crscode
                                                            == proposed], ",")))
          {
            choicesets <-
              sdr_generate_choicesets(con,
                                      schema,
                                      proposed,
                                      existing = FALSE,
                                      abs_crs = at_risk)

            # Generate probability table
            sdr_generate_probability_table(con,
                                           schema,
                                           choicesets,
                                           paste0(tolower(at_risk),
                                                  "_after_abs_",
                                                  tolower(proposed)))
            # make frequency group adjustments if required
            if (isTRUE(have_freqgroups) &
                (!is.na(stations$freqgrp[stations$crscode == proposed]))) {
              df <-
                data.frame(fgrp = freqgroups[freqgroups$group_id ==
                                               stations$freqgrp[stations$crscode == proposed],
                                             "group_crs"], stringsAsFactors = FALSE)
              sdr_frequency_group_adjustment(con,
                                             schema,
                                             df,
                                             paste0(
                                               tolower(at_risk),
                                               "_after_abs_",
                                               tolower(proposed)
                                             ))
            }
            # calculate probabilities
            sdr_calculate_probabilities(con,
                                        schema,
                                        paste0(tolower(at_risk),
                                               "_after_abs_",
                                               tolower(proposed)))
            # get probability weighted population
            prweighted_pop_after <-
              sdr_calculate_prweighted_population(con,
                                                  schema,
                                                  at_risk,
                                                  paste0(tolower(at_risk),
                                                         "_after_abs_",
                                                         tolower(proposed)))
            # update abstraction_results table
            query <- paste0(
              "update ",
              schema,
              ".abstraction_results
          set prwpop_after = ",
              prweighted_pop_after,
              " where proposed = '",
              proposed,
              "' and at_risk = '",
              at_risk,
              "'"
            )
            sdr_dbExecute(con, query)
            query <- paste0(
              "update ",
              schema,
              ".abstraction_results
          set change = prwpop_after - prwpop_before,
          pc_change = ((prwpop_after - prwpop_before::real) / prwpop_before) * 100
          where proposed = '",
              proposed,
              "' and at_risk = '",
              at_risk,
              "'"
            )
            sdr_dbExecute(con, query)
            # generate GeoJSON catchments
            sdr_create_json_catchment(
              con,
              schema,
              "abstraction",
              pcpoly$exists,
              at_risk,
              paste0(tolower(at_risk),
                     "_after_abs_",
                     tolower(proposed)),
              proposed,
              tolerance = 2
            )
          }
        }
      } else {
        # For the concurrent method, abstraction entry MUST be same for all proposed
        # stations when submitted (this is checked in pre-flight). So we just need to
        # get the entry for the first row - if not NA - and loop through each of the
        # at_risk stations.
        for (at_risk in na.omit(unlist(strsplit(stations$abstract[1], ",")))) {
          # for each at-risk station we pass all the proposed stations to the function
          # - as all would be present in the after situation in concurrent mode
          choicesets <-
            sdr_generate_choicesets(con,
                                    schema,
                                    stations$crscode,
                                    existing = FALSE,
                                    abs_crs = at_risk)
          # Generate probability table
          sdr_generate_probability_table(con,
                                         schema,
                                         choicesets,
                                         paste0(tolower(at_risk),
                                                "_after_abs_concurrent"))
          # Make frequency group adjustments if required.
          # Only a single identical frequency group for all stations under
          # concurrent treatment. This is checked during pre-flight. So we just use
          # the first row
          if (isTRUE(have_freqgroups) & (!is.na(stations$freqgrp[1]))) {
            df <-
              data.frame(fgrp = freqgroups[freqgroups$group_id ==
                                             stations$freqgrp[1], "group_crs"],
                         stringsAsFactors = FALSE)
            sdr_frequency_group_adjustment(con,
                                           schema,
                                           df,
                                           paste0(tolower(at_risk),
                                                  "_after_abs_concurrent"))
          }
          # calculate probabilities
          sdr_calculate_probabilities(con,
                                      schema,
                                      paste0(tolower(at_risk),
                                             "_after_abs_concurrent"))
          # get probability weighted population
          prweighted_pop_concurrent <-
            sdr_calculate_prweighted_population(con,
                                                schema,
                                                at_risk,
                                                paste0(tolower(at_risk),
                                                       "_after_abs_concurrent"))
          # update abstraction_results table
          query <- paste0(
            "update ",
            schema,
            ".abstraction_results
        set prwpop_after = ",
            prweighted_pop_concurrent,
            " where proposed = 'concurrent' and at_risk = '",
            at_risk,
            "'"
          )
          sdr_dbExecute(con, query)
          query <- paste0(
            "update ",
            schema,
            ".abstraction_results
        set change = prwpop_after - prwpop_before,
        pc_change = ((prwpop_after - prwpop_before::real) / prwpop_before) * 100
        where proposed = 'concurrent' and at_risk = '",
            at_risk,
            "'"
          )
          sdr_dbExecute(con, query)
          # generate GeoJSON catchments
          sdr_create_json_catchment(
            con,
            schema,
            "abstraction",
            pcpoly$exists,
            at_risk,
            paste0(tolower(at_risk), "_after_abs_concurrent"),
            "concurrent",
            tolerance = 2
          )
        }
      } # end concurrent

      # For concurrent and isolation modes
      # Calculate change between the before and after situation
      query <- paste0(
        "update ",
        schema,
        ".abstraction_results
    set adj_trips = round(entsexits + ((pc_change / 100) * entsexits))"
      )
      sdr_dbExecute(con, query)
      query <- paste0("update ",
                      schema,
                      ".abstraction_results
    set trips_change = adj_trips - entsexits")
      sdr_dbExecute(con, query)

    } # end abstraction analysis

    # Pulling outputs---------------------------------------------------------------

    flog.info("Retrieving station forecast(s) from database")

    # stations

    query <-
      paste0(
        "select crscode as id, name, region, stn_east as station_easting, stn_north as station_northing,
acc_east as access_easting, acc_north as access_northing, freq as frequency, freqgrp as frequency_group, carsp as parking_spaces, ticketm as ticket_machine, busint as bus_interchange, cctv, terminal as terminal_station, tcbound as travelcard_boundary, category, abstract as abstraction_stations, workpop_2min as work_population_2mins, prw_pop as weighted_population, forecast_base, forecast_uplift
from ",
        schema,
        ".proposed_stations"
      )
    out_stations <- sdr_dbGetQuery(con, query)
    write.csv(
      out_stations,
      file.path(out_path, "station_forecast.csv", fsep = .Platform$file.sep)
    )

    # abstraction analysis

    if (length(unique(na.omit(stations$abstract))) > 0) {
      flog.info("Retrieving abstraction analysis from database")
      query <- paste0(
        "select proposed as proposed_id, proposed_name, at_risk as impacted_station, prwpop_before, prwpop_after, change, pc_change, entsexits as entries_exits, adj_trips, trips_change
from ",
        schema,
        ".abstraction_results"
      )
      out_abstract <- sdr_dbGetQuery(con, query)
      write.csv(
        out_abstract,
        file.path(out_path, "abstraction_analysis.csv", fsep = .Platform$file.sep)
      )
    }

    # Exogenous table
    query <- paste0(
      "select type, number, centroid, population, avg_hhsize from ",
      schema,
      ".exogenous_input"
    )
    out_exogenous <- sdr_dbGetQuery(con, query)
    write.csv(
      out_exogenous,
      file.path(out_path, "exogenous_inputs.csv", fsep = .Platform$file.sep)
    )

    # Station catchments

    flog.info("Retrieving station catchment(s)")

    query <-
      paste0("select crscode, catchment from ",
             schema,
             ".proposed_stations")
    out_catchments <- sdr_dbGetQuery(con, query)

    for (crscode in out_catchments$crscode) {
      write(
        out_catchments$catchment,
        file.path(
          out_path,
          paste0(tolower(crscode), "_catchment.geojson"),
          fsep = .Platform$file.sep
        )
      )
    }

    # Abstraction catchments

    if (length(unique(na.omit(stations$abstract))) > 0) {
      flog.info("Retrieving abstraction analysis catchments")
      query <- paste0(
        "select proposed, at_risk, catchment_before, catchment_after from ",
        schema,
        ".abstraction_results"
      )
      out_abscatchments <- sdr_dbGetQuery(con, query)

      for (crscode in out_abscatchments$proposed) {
        write(
          out_abscatchments$catchment_before,
          file.path(
            out_path,
            paste0(
              tolower(crscode),
              "_",
              tolower(at_risk),
              "_catchment_before.geojson"
            ),
            fsep = .Platform$file.sep
          )
        )
      }

      for (crscode in out_abscatchments$proposed) {
        write(
          out_abscatchments$catchment_after,
          file.path(
            out_path,
            paste0(
              tolower(crscode),
              "_",
              tolower(at_risk),
              "_catchment_after.geojson"
            ),
            fsep = .Platform$file.sep
          )
        )
      }

    }

    # Closing actions---------------------------------------------------------------

    flog.info("Tidying up")

    stopCluster(cl)
    dbDisconnect(con)

    flog.info("Job finished")

  }

library(keyring)
library(RPostgres)

con <-
  dbConnect(
    RPostgres::Postgres(),
    dbname = "dafni",
    host = "localhost",
    user = "postgres",
    port = "5432",
    password = key_get("postgres")
  )

# This script parses the ATOC Stations XML Feed to extract information on station services and facilities
# and then updates a postgresql table.

# Load the required libraries
library(RCurl)
library(httr)
library(XML)
library(jsonlite)

# define namespaces vector
ns <-
  c(x = "http://nationalrail.co.uk/xml/station", y = "http://nationalrail.co.uk/xml/common", z = "http://www.govtalk.gov.uk/people/AddressAndPersonalDetails")

# need to get authentication token
# use postForm() from RCurl

url <- "https://opendata.nationalrail.co.uk/authenticate"
token <- postForm(url,
                  username = "m.a.young@soton.ac.uk",
                  password = "GLb,zB?hA83a=,Pw",
                  style = "POST")
asjson <- fromJSON(token)

stations_nre <-
  GET(
    "https://opendata.nationalrail.co.uk/api/staticfeeds/4.0/stations",
    add_headers('X-Auth-Token' = asjson$token)
  )

xml_stations <- xmlParse(stations_nre)

all_crs <-
  unlist(xmlToDataFrame(
    getNodeSet(xml_stations, "//x:Station/x:CrsCode", namespaces = ns),
    stringsAsFactors = FALSE
  ), use.names = FALSE)


# start loop
for (crscode in all_crs) {
  path <- paste0("//x:Station[x:CrsCode='", crscode, "']")

  xml_crs <- getNodeSet(xml_stations, path , namespaces = ns)
  xml_node <- newXMLNode("StationList")
  xml <- addChildren(xml_node, kids = xml_crs)

  # initialise list
services <- vector(mode = "list", length = 8)
  # set list names
  names(services) <-
    c(
      "crscode",
      "name",
      "longitude",
      "latitude",
      "staffinglevel",
      "cctv",
      "ticketmachine",
      "carspaces"
    )

  services$crscode <- crscode

  # get Name
  services$name <-
    toupper(xpathSApply(xml,
                "/StationList/x:Station/x:Name",
                namespaces = ns,
                fun = xmlValue))

  # get Longitude
  services$longitude <-
    xpathSApply(
      xml,
      "/StationList/x:Station/x:Longitude",
      namespaces = ns,
      fun = xmlValue
    )

  # get Latitude
  services$latitude <-
    xpathSApply(xml,
                "/StationList/x:Station/x:Latitude",
                namespaces = ns,
                fun = xmlValue)

  # get staffingLevel - mandatory fullTime, partTime or unstaffed
  services$staffinglevel <-
    xpathSApply(
      xml,
      "/StationList/x:Station/x:Staffing/x:StaffingLevel",
      namespaces = ns,
      fun = xmlValue
    )

  # get CCTV status - mandatory TRUE or FALSE
  services$cctv <-
    xpathSApply(
      xml,
      "/StationList/x:Station/x:Staffing/x:ClosedCircuitTelevision/x:Available",
      namespaces = ns,
      fun = xmlValue
    )

  # get ticketMachine available - optional true/false - if tag missing assume false as per schema
  xpath <-
    "/StationList/x:Station/x:Fares/x:TicketMachine/x:Available"
  if (length(xpathSApply(xml, xpath, namespaces = ns)) > 0) {
    services$ticketmachine <-
      xpathSApply(xml, xpath, namespaces = ns, fun = xmlValue)
  } else {
    services$ticketmachine <- "false"
  }

  # get total car parking spaces - optional
  xpath <-
    "/StationList/x:Station/x:Interchange/x:CarPark/x:Spaces"
  # Check at least one Spaces tag exists
  if (length(xpathSApply(xml, xpath, namespaces = ns)) > 0) {
    #Use Reduce to sum the spaces for all carparks tags
    services$carspaces <-
      Reduce(sum, (as.numeric(
        xpathSApply(xml, xpath, namespaces = ns, fun = xmlValue)
      )))
  } else {
    services$carspaces <- 0
  }

  # get Bus Services - optional true/false/unknown
  # no longer appears to be recorded in the KB as of 30 April 2019.
  # xpath <- "/StationList/x:Station/x:Interchange/x:BusServices/y:Available"
  # # Check if tag exists
  # if (length(xpathSApply(xml, xpath, namespaces = ns)) > 0) {
  #   # get tag value
  #   services$busServices <-
  #     xpathSApply(xml, xpath, namespaces = ns, fun = xmlValue)
  # } else {
  #   services$busServices <- "NA"
  # }

   # Write results to database table
  dbWriteTable(
    conn = con,
    Id(schema = "data", table = "stations_nrekb"),
    data.frame(t(unlist(services))),
    append = TRUE,
    row.names = FALSE
  )
  # end the loop
}

# Add primary key

query <- paste0("
                alter table data.stations_nrekb
                add primary key (crscode)
                ")
dbExecute(con, query)


# Add busservices

query <- paste0("
                alter table data.stations_nrekb
                add column busservices text
                ")
dbExecute(con, query)

# update from data.stations_full

query <- paste0("
                update data.stations_nrekb a
                set busservices = b.busservices
                from data.stations_full b
                where a.crscode = b.crscode
                ")
dbExecute(con, query)

# Add category

query <- paste0("
                alter table data.stations_nrekb
                add column category text
                ")
dbExecute(con, query)

# update from data.stations_full

query <- paste0("
                update data.stations_nrekb a
                set category = b.category
                from data.stations_full b
                where a.crscode = b.crscode
                ")
dbExecute(con, query)


# update frequency from latest GTFS source

# see load_gtfs_database.R
# see generate_station_frequencies_gtfs_2019.R

# Remove any stations where frequency is zero
# these should probably be manually checked
# usual reasons are:
# no weekday service
# station is currently under construction
# station has closed

query <- paste("
                delete from data.stations_nrekb
                where frequency = 0")
query <- gsub(pattern = '\\s',replacement = " ",x = query)
dbExecute(con, query)

# remove isle of weight stations

query <- paste(
  "delete from data.stations_nrekb
                where crscode in ('BDN', 'LKE', 'RYD', 'RYP', 'RYR', 'SAB', 'SAN', 'SHN')"
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbExecute(con, query)

# remove serves airport only

query <- paste(
  "delete from data.stations_nrekb
                where crscode in ('HAF', 'HWV', 'HXX')"
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbExecute(con, query)

# remove no public access

query <- paste(
  "delete from data.stations_nrekb
                where crscode in ('LYC', 'RBS', 'SNT')"
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbExecute(con, query)


# remove limited/no vehicle access

query <- paste(
  "delete from data.stations_nrekb
                where crscode in ('BYA', 'ABC', 'CRR')"
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbExecute(con, query)

# remove the Dublin Stena line and ferryport crscodes

query <- paste(
  "delete from data.stations_nrekb
                where crscode in ('DPS', 'DFP')"
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbExecute(con, query)


# create geom column for location

query <- paste(
  "alter table data.stations_nrekb
  add column location_geom geometry(Point,27700) "
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbExecute(con, query)


# update station category for new stations

query <- paste(
  "update data.stations_nrekb
  set category = 'C' where crscode in ('MRW', 'CMB')"
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbExecute(con, query)


query <- paste(
  "update data.stations_nrekb
  set category = 'E' where crscode in ('MNS', 'KNW', 'EGY')"
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbExecute(con, query)

query <- paste(
  "update data.stations_nrekb
  set category = 'F' where crscode in ('ILN', 'LMR')"
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbExecute(con, query)

query <- paste(
  "update data.stations_nrekb
  set busservices = 'true' where crscode in ('MRW', 'MNS', 'KNW', 'CMB', 'ILN')"
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbExecute(con, query)

query <- paste(
  "update data.stations_nrekb
  set busservices = 'false' where crscode in ('LMR', 'EGY')"
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbExecute(con, query)


# create geometry

query <- paste(
  "	update data.stations_nrekb
  set location_geom = ST_Transform(ST_SetSrid(ST_MakePoint(longitude::numeric, latitude::numeric),4326), 27700)
  "
)
query <- gsub(pattern = '\\s',
              replacement = " ",
              x = query)
dbExecute(con, query)

# create location

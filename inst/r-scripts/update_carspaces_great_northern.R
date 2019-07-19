

great_northern_parking_spaces <- read.csv(file = "inst/source_data/gn_parking_spaces.csv", stringsAsFactors = FALSE)

crscodes <- great_northern_parking_spaces$crscode
spaces <- great_northern_parking_spaces$carspaces
qry = "update data.stations set carspaces = $1 where crscode = $2"
dbSendQuery(con, qry, list(spaces, crscodes))

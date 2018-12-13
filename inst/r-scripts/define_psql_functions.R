
# postgresql pgrouting functions


# bbox_pgr_dijkstra
# Expands pgr_dijkstra enabling a bbox to be defined
# Needed to speed up queries
# Also incorporates pgr_pointtoedgenode to find nearest node on nearest edge
# Function is created in schema openroads

query <- paste0(getSQL("inst/sql/function_bbox_pgr_dijkstra.sql"))
dbGetQuery(con, query)



# bbox_pgr_withpoints
# Expands pgr_withpoints enabling a bbox to be defined
# Needed to speed up queries
# Function is created in schema openroads

query <- paste0(getSQL("inst/sql/function_bbox_pgr_withpoints.sql"))
dbGetQuery(con, query)

# create_pgr_nodes
# Adapted from code block 11.2 from "pgROUTING - a practical guide".
# Amended to return the geom of the virtual nodes (needed for enhanced bbox functions),
# or otherwise the geom of a real node if fraction <0.01 (edge start) or >0.99 (edge end)
# Returns virtual node pids and fraction, or if virtual node is at the same location as a source
# or target real node (i.e either end of an edge) the source or target node pid
# is returned (otherwise routing would not work).
# takes array of points (whihc can be a single point)
# may not need to return pid if populating a node table with serial id.

query <- paste0(getSQL("inst/sql/function_create_pgr_vnodes.sql"))
dbGetQuery(con, query)


# function to obtain the nearest 10 railway stations to given origin
# currently uses bbox_pgr_dijkstracost
# to be amended to use bbox_pgr_withpointscost
# also need additional parameters
# select * from sdr_nearest_stations(st_setsrid(st_point(288541,	60740), 27700), 1)

query <- paste0(getSQL("inst/sql/function_sdr_nearest_stations.sql"))
dbGetQuery(con, query)

# function to obtain nearest 10 railways stations to given origin
# using bbox_pgr_withpointscost

query <- paste0(getSQL("inst/sql/function_sdr_nearest_stationswithpoints.sql"))
dbGetQuery(con, query)

## function to find nearets 10 stations using just postcode (text) as orign
query <- paste0(getSQL("inst/sql/function_sdr_pc_nearest_stationswithpoints.sql"))
dbGetQuery(con, query)

## function to return distance only between a named postcode and a station
## to be used to exapnd bbox on a one-off basis for any postcode:station pairs
## where the distance returned null due to limits of bbox.
query <- paste0(getSQL("inst/sql/sdr_pc_station_withpoints.sql"))
dbGetQuery(con, query)





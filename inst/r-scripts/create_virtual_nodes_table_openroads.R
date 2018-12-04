## create a table of virtual nodes for roadlinks
## node pids are created as negative
## these nodes are intended to be used in pgr_withpointsdd and the bboxversions
## overcomes issue of long roads with no nodes - e.g. motorways
## better service areas therefore produced

query <- paste0("create sequence openroads.vnodes_pid_seq;")
dbGetQuery(con, query)

# alter sequence start - so virtual node pids are unique from real nodes
query <- paste0("alter vnodes_pid_seq restart 10000000;")
dbGetQuery(con, query)

# create vnodes table containing columns consistent with pgr requirements
# Additional creates geom column needed for bbox expanded functions
# virtual nodes are created on edges > 1500 km and added every 1km

query <- paste0(getSQL("inst/sql/create_virtual_nodes_openroads.sql"))
dbGetQuery(con, query)

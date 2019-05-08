
# stationdemandr 0.1.0.9000

## Fixes

* Fixed bug with parallel logging
* Fixed bug in sdr_create_service_area(). ST_ConcaveHull can return a point or line
in some circumstances. Need to apply a buffer to ensure sa is a polygon and can be
written to table.
* Fixed bug in SQL function sdr_crs_pc_nearest_stationswithpoints() - in the select
case 60km and 80km service areas were incorrectly specified as 30km and 40km.

## New features

* Import boolean variables as logical - ensures they are all TRUE or FALSE which
is also valid boolean PostgreSQL values

## Performance improvements

* Views previously used in sdr_generate_choicesets() replaced with materialized views
* Various indexes (incl. spatial) now created for the materialized views

# stationdemandr 0.1.0

Initial release

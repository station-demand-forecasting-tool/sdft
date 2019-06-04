# stationdemandr 0.2.3.9000

## Fixes

- added new script `start.R`. This simply calls main.R using `source()`. `start.R` can
then be run from the terminal using rscript. If main.R is called directly using 
rscript then `stop()` calls will not terminate the model.

- problem if category was 'F'. Type was not specified when proposed_stations table
written to database. When category was 'F' type Boolean was assumed by default. Must
be type text.

# stationdemandr 0.2.3

## Performance improvements

- Modified `sdr_create_json_catchment()` to improve catchment display performance.
Probability is now rounded to 1 decimal places and then st_union is used to 
dissolve the postcode polygons, grouped by probability. The GeoJSON is then
generated. This means that any catchment will now have a maximum feature count 
of 10. Also defined `maxdecimaldigits` for `st_asgeojson`, set to 5 rather than 
the default of 15.


# stationdemandr 0.2.2

## Fixes

- Fixed issue with the BEFORE abstraction analysis. Frequency group adjustments 
were not applied to the *before* situation - there was a single shared
probability table for each at_risk station. This appeared to make sense when the
decision was made. However, if one of the at-risk stations is also subject to an
upward frequency adjustment then its weighted population will increase in the
*after* situation (because of the higher frequency variable). This has been
amended so that a separate probablity table is created for each 
proposed_station:at_risk station pair (the same as for the *after* analysis) and
frequency group adjustments are made as required.

# stationdemandr 0.2.1

## Fixes

- Fixed bug with main script not able to handle empty data frames resulting from 
import of empty exogenous.csv and freqgroups.csv input files (which are optional).

# stationdemandr 0.2.0

## Fixes

- Fixed bug with parallel logging
- Fixed bug in `sdr_create_service_area()`. `ST_ConcaveHull` can return a point or line
in some circumstances. Need to apply a buffer to ensure sa is a polygon and can be
written to table.
- Fixed bug in SQL function `sdr_crs_pc_nearest_stationswithpoints()` - in the select
case 60km and 80km service areas were incorrectly specified as 30km and 40km.
- Regexp for job_id pre-flight check amended to allow  20 character length.

## New/amended features

- Import Boolean variables as logical - ensures they are all TRUE or FALSE which
are also valid Boolean PostgreSQL values.
- Argument **columns** added to `sdr_create_service_areas()` (logical) to indicate
whether the service area columns should be created. Increases flexibility of
function.
- Column name changed from **dailyfrequency_2017_all** to **frequency** in stations 
table and `sdr_generate_probability_table()` function amended.
- Column name changed from **entsexits2017** to **entsexits** in stations table and
main
script amended appropriately.
- Additional pre-flight check for **region** field.
- `sapply` replaced with `vapply` throughout. Preferred as `sapply` is not type safe.

## Performance improvements

- Views previously used in `sdr_generate_choicesets()` replaced with materialized
views.
- Various indexes (incl. spatial) now created for the materialized views.

## Data issues

- Removed any edges and nodes disconnected from the main network graph from the 
OS OpenRoads tables. The affected nodes were identified using the pgRouting 
function `pgr_connectedComponents()` and were then used to delete affected edges from 
openroads.roadlinks and nodes from openroads.roadnodes. This deals with potential
problem of the nearest edge to a postcode or station not being connected to the 
rest of the road network (a potential issue for routing and building station 
service areas). Centroidnodes and roadlink virtual node tables regenerated. Example
disconnected network (in green) shown below:
<p align="center">
  <img src="man/figures/disconnected_network.jpg"/>
</p>

- Full stations data created (up-to-date with latest station openings and closures).
- Service frequency updated from latest timetable data.
- Services and facilities updated from NRE KB.
- Full postcode centroid and population data.
- Full workplace zone centroid and population data. Also includes workplace zones
for Scotland (previously not available from Census Scotland - postcode centroids
previously used).

# stationdemandr 0.1.0

Initial release




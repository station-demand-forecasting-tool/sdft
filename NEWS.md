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
- `sapply` replaced with `vapply` throughout. Better as `sapply` is not type safe.

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
service areas). Centroidnodes and roadlink virtual node tables regenerated.

<p align="center">
  <img src="man/figures/disconnected_network.jpg"/>
  <figcaption align="center">Example of a disconnected network (in green)</figcaption>
</p>

- Full stations data created (up-to-date with latest station openings and closures).
- Full postcode centroid and population data.
- Full workplace zone centroid and population data. Also includes workplace zones
for Scotland (previously not available from Census Scotland - postcode centroids
previously used).

# stationdemandr 0.1.0

Initial release




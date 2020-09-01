# Station Demand Forecasting Tool

This is the R package for the Station Demand Forecasting Tool.

## About the tool

This tool generates a demand forecast (predicted trips per year) for one or more proposed local railway stations in mainland GB. If required it can also produce an analysis of potential abstraction of journeys from existing stations, enabling the net impact of a new station on rail use to be estimated. Forecasts for multiple stations can be accommodated as part of the same job. These can be treated independently (alternative station locations are to be assessed) or concurrently (the proposed stations will coexist).

The underlying model is based on research by [Marcus Young](https://www.southampton.ac.uk/engineering/about/staff/may1y17.page) at the University of Southampton’s [Transportation Research Group](https://www.southampton.ac.uk/engineering/research/groups/transportation_group.page). At its core is a trip end model which has been calibrated on the smaller stations (network Rail Categories E and F) in Great Britain. In such a model the number of trips is a function of the population in a station’s catchment and a range of other variables, such as service frequency and number of jobs nearby. A novel aspect of this model is that probability-based catchments are defined at the unit postcode level using a station choice model. Rather than assuming everyone will use their nearest station, this provides a more realistic representation of behaviour and allows competition to occur between stations.

A [conference paper](https://eprints.soton.ac.uk/432493/) with more details about the model is available. Note that the web front-end referred to in this paper is not part of the code release.

## Tool implementation

While this R package controls the model, ***it is not standalone***. Most of the heavy lifting takes place in a PostgreSQL database that requires setting up with data tables and additional wrapper functions for pgRouting.

It is planned to provide a [Docker container](https://github.com/station-demand-forecasting-tool/sdft-docker) for the database component to enable easy deployment of the tool.

## Tool documentation

[https://www.stationdemand.org.uk](https://www.stationdemand.org.uk)

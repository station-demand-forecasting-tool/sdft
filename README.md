# Station Demand Forecasting Tool

<!-- badges:  start -->
[![Build Status](https://travis-ci.org/station-demand-forecasting-tool/sdft.svg?branch=master)](https://travis-ci.org/station-demand-forecasting-tool/sdft)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.4065250.svg)](https://doi.org/10.5281/zenodo.4066925)
[![Project Status: Active – The project has reached a stable, usable state and is being actively developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)
<!-- badges:  end -->

This is the R package for the Station Demand Forecasting Tool.

## About the tool

This tool generates a demand forecast (predicted trips per year) for one or more proposed local railway stations in mainland GB. If required it can also produce an analysis of potential abstraction of journeys from existing stations, enabling the net impact of a new station on rail use to be estimated. Forecasts for multiple stations can be accommodated as part of the same job. These can be treated independently (alternative station locations are to be assessed) or concurrently (the proposed stations will coexist).

The underlying model is based on research by [Marcus Young](https://www.southampton.ac.uk/engineering/about/staff/may1y17.page) at the University of Southampton’s [Transportation Research Group](https://www.southampton.ac.uk/engineering/research/groups/transportation_group.page). At its core is a trip end model which has been calibrated on the smaller stations (network Rail Categories E and F) in Great Britain. In such a model the number of trips is a function of the population in a station’s catchment and a range of other variables, such as service frequency and number of jobs nearby. A novel aspect of this model is that probability-based catchments are defined at the unit postcode level using a station choice model. Rather than assuming everyone will use their nearest station, this provides a more realistic representation of behaviour and allows competition to occur between stations.

A [conference paper](https://eprints.soton.ac.uk/432493/) with more details about the model is available. Note that the web front-end referred to in this paper is not part of the code release.

## Tool implementation

While this R package controls the tool, ***it is not standalone***. Most of the heavy lifting takes place in a PostgreSQL database that requires setting up with data tables and additional wrapper functions for pgRouting.

A [Docker implementation](https://github.com/station-demand-forecasting-tool/sdft-docker) is available. 

## Tool documentation

[https://www.stationdemand.org.uk](https://www.stationdemand.org.uk).

## How to cite

Please cite **sdft** if you use it. Get citation information using: `citation(package = 'sdft')`:

```
To cite the sdft package in publications, please use the following. You can obtain
the DOI for a specific version from: https://zenodo.org/record/4066925

  Marcus Young and Simon Blainey (2020). sdft: The Station Demand Forecasting Tool.
  R package version 0.3.1. https://doi.org/10.5281/zenodo.4066925

A BibTeX entry for LaTeX users is

  @Manual{,
    author = {{Marcus Young} and {Simon Blainey}},
    title = {{sdft: The Station Demand Forecasting Tool}},
    year = {2020},
    note = {{R package version 0.3.1}},
    doi = {{10.5281/zenodo.4066925}},
  }

```


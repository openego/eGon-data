Information about electricity demands and their spatial and temporal aggregation 

.. _disagg-cts-elec-ref:

Spatial disaggregation of CTS demand to buildings
+++++++++++++++++++++++++++++++++++++++++++++++++++

The spatial disaggregation of the annual CTS demand to buildings is conducted in the dataset
:py:class:`CtsDemandBuildings <egon.data.datasets.electricity_demand_timeseries.cts_buildings.CtsDemandBuildings>`.
Both the electricity demand as well as the heat demand is disaggregated
in the dataset. Here, only the disaggregation of the electricity demand is described.
The disaggregation of the heat demand is analogous to it. More information on the resulting
tables is given in section :ref:`heat-demand-ref`.

The workflow generally consists of three steps. First, the annual demand from
Peta5 [Peta]_ is used to identify census cells with demand.
Second, Openstreetmap [OSM]_ data on buildings and amenities is used to map the demand to single buildings.
If no sufficient OSM data are available, new synthetic buildings and if necessary
synthetic amenities are generated.
Third, each building's share of the HV-MV substation demand profile is determined
based on the number of amenities within the building and the census cell(s) it is in.

The workflow is in more detail shown in figure
:ref:`disaggregation-cts-model` and described in the following.

.. figure:: /images/flowchart_cts_disaggregation.jpg
  :name: disaggregation-cts-model
  :width: 800

  Workflow for the disaggregation of the annual CTS demand to buildings

In the :py:class:`OpenStreetMap <egon.data.datasets.osm.OpenStreetMap>` dataset, we filtered all
OSM buildings and amenities for tags we relate to the CTS sector. Amenities are mapped
to intersecting buildings and then intersected with the annual demand at census cell level. We obtain
census cells with demand that have amenities within and census cells with demand that
don't have amenities within.
If there is no data on amenities, synthetic ones are assigned to existing buildings. We use
the median value of amenities per census cell in the respective MV grid district
to determine the number of synthetic amenities.
If no building data is available, a synthetic building with a dimension of 5x5 m is randomly generated.
This also happens for amenities that couldn't be assigned to any OSM building.
We obtain four different categories of buildings with amenities:

* Buildings with amenities
* Synthetic buildings with amenities
* Buildings with synthetic amenities
* Synthetic buildings with synthetic amenities

All buildings with CTS, comprising OSM buildings and synthetic buildings, including
the number of amenities within the building are written to database table
:py:class:`openstreetmap.egon_cts_buildings <egon.data.datasets.electricity_demand_timeseries.cts_buildings.CtsBuildings>`.

To determine each building's share of the HV-MV substation demand profile,
first, the share of each building on the demand per census cell is calculated
using the number of amenities per building.
Then, the share of each census cell on the demand per HV-MV substation is determined
using the annual demand defined by Peta5.
Both shares are finally multiplied and summed per building ID to determine each
building's share of the HV-MV substation demand profile. The summing per building ID is
necessary, as buildings can lie in multiple census cells and are therefore assigned
a share in each of these census cells.
The share of each CTS building on the CTS electricity demand profile per HV-MV substation
in each scenario is saved to the database table
:py:class:`demand.egon_cts_electricity_demand_building_share <egon.data.datasets.electricity_demand_timeseries.cts_buildings.EgonCtsElectricityDemandBuildingShare>`.
The CTS electricity peak load per building is written to database table
:py:class:`demand.egon_building_electricity_peak_loads <egon.data.datasets.electricity_demand_timeseries.hh_buildings.BuildingElectricityPeakLoads>`.

Drawbacks and limitations as well as assumptions and challenges of the disaggregation
are discussed in the dataset docstring of
:py:class:`CtsDemandBuildings <egon.data.datasets.electricity_demand_timeseries.cts_buildings.CtsDemandBuildings>`.

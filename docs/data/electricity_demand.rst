.. _elec_demand_ref:
The electricity demand considered includes demand from the residential, commercial and industrial sector. 
The target values for scenario *eGon2035* are taken from the German grid development plan from 2021 [NEP2021]_,
whereas the distribution on NUTS3-levels corresponds to the data from the research project *DemandRegio* [demandregio]_. 
The following table lists the electricity demands per sector: 

.. list-table:: Electricity demand per sector
   :widths: 25 50
   :header-rows: 1
   
   * - Sector
     - Annual electricity demand in TWh
   * - residential
     - 115.1
   * - commercial 
     - 123.5  
   * - industrial
     - 259.5
     
A further spatial and temporal distribution of the electricity demand is needed to fullfil all requirements of the 
subsequent grid optimization. Therefore different, sector-specific distributions methods were developed and applied. 

Residential electricity demand
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The annual electricity demands of households on NUTS3-level from *DemandRegio* are scaled to meet the national target 
values for the respective scenario in dataset :py:class:`DemandRegio <egon.data.datasets.demandregio.DemandRegio>`. 
A further spatial and temporal distribution of residential electricity demands is performed in 
:py:class:`HouseholdElectricityDemand <egon.data.datasets.electricity_demand.HouseholdElectricityDemand>` as described 
in [Buettner2022]_.
The result is a consistent dataset across aggregation levels with an hourly resolution. 

.. figure:: /images/S27-3.png
  :name: spatial_distribution_electricity_demand
  :width: 400
  
  Electricity demand on NUTS 3-level (upper left); Exemplary MVGD (upper right); Study region in Flensburg (20 Census cells, bottom) from [Buettner2022]_


.. figure:: /images/S27-4a.png
  :name: aggregation_level_electricity_demand
  :width: 400
  
  Electricity demand time series on different aggregation levels from [Buettner2022]_



Commercial electricity demand
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The distribution of electricity demand from the commercial, trade and service (CTS) sector is also based on data from 
*DemandRegio*, which provides annual electricity demands on NUTS3-level for Germany. In  dataset 
:py:class:`CtsElectricityDemand <egon.data.datasets.electricity_demand.CtsElectricityDemand>` the annual electricity
demands are further distributed to census cells (100x100m cells from [Census]_) based on the distribution of heat demands, 
which is taken from the Pan-European Thermal Altlas version 5.0.1 [Peta]_. For further information refer to section 
ref:`heat_demand`.
The applied methods for a futher spatial and temporal distribution to buildings is described in [Buettner2022]_ and 
performed in dataset :py:class:`CtsDemandBuildings <egon.data.datasets.electricity_demand_timeseries.CtsDemandBuildings>`

Industrial electricity demand
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To distribute the annual industrial electricity demand OSM landuse data as well as information on industrial sites are 
taken into account. 
In a first step (:py:class:`CtsElectricityDemand <egon.data.datasets.electricity_demand.CtsElectricityDemand>`)
different sources providing information about specific sites and further information on the  industry sector in which 
the respective industrial site operates are combined. Here, the three data sources [Hotmaps]_, [sEEnergies]_ and 
[Schmidt2018]_ are aligned and joined.  
Based on the resulting list of industrial sites in Germany and information on industrial landuse areas from OSM [OSM]_
which where extracted and processed in :py:class:`OsmLanduse <egon.data.datasets.loadarea.OsmLanduse>` the annual demands
were distributed.
The spatial and temporal distribution is performed in 
:py:class:`IndustrialDemandCurves <egon.data.datasets.industry.IndustrialDemandCurves>`. 
For the spatial distribution of annual electricity demands from *DemandRegio* [demandregio]_ which are available on 
NUTS3-level are in a first step evenly split 50/50 between industrial sites and OSM-polygons tagged as industrial areas.
Per NUTS-3 area the respective shares are then distributed linearily based on the area of the corresponding landuse polygons 
and evenly to the identified industrial sites.
In a next step the temporal disaggregation of the annual demands is carried out taking information about the industrial
sectors and sector-specific standard load profiles from [demandregio]_ into account.
Based on the resulting time series and their peak loads the corresponding grid level and grid connections point is 
identified. 

Electricity demand in neighbouring countries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The neighbouring countries considered in the model are represented in a lower spatial resolution of one or two buses per
country. The national demand timeseries in an hourly resolution of the respective countries is taken from the Ten-Year 
Network Development Plan, Version 2020 [TYNDP]_. In case no data for the target year is available the data is is
interpolated linearly.  
Refer to the corresponding dataset for detailed information: 
:py:class:`ElectricalNeighbours <egon.data.datasets.ElectricalNeighbours>`

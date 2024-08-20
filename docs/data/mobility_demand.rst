.. _mobility-demand-mit-ref:

Motorized individual travel
++++++++++++++++++++++++++++

The electricity demand data of motorized individual travel (MIT) for both the eGon2035
and eGon100RE scenario is set up
in the :py:class:`MotorizedIndividualTravel<egon.data.datasets.emobility.motorized_individual_travel.MotorizedIndividualTravel>`
dataset.

The profiles are generated using a modified version of
`SimBEV v0.1.3 <https://github.com/rl-institut/simbev/tree/1f87c716d14ccc4a658b8d2b01fd12b88a4334d5>`_.
SimBEV generates driving profiles for battery electric vehicles (BEVs) and
plug-in hybrid electric vehicles (PHEVs) based on MID survey data [MiD2017]_ per
RegioStaR7 region type [RegioStaR7_2020]_.
These profiles include driving, parking and (user-oriented) charging times.
Different vehicle classes are taken
into account whose assumed technical data is given in table :ref:`ev-types-data`.
Moreover, charging probabilities for multiple types of charging
infrastructure are presumed based on [NOW2020]_ and [Helfenbein2021]_.
Given these assumptions, a pool of 33.000 EVs-types is pre-generated and provided through the data bundle
(see :ref:`data-bundle-ref`) as well as written
to table :py:class:`EgonEvTrip<egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvTrip>`.
The complete tech data and assumptions of the run can be found in the
metadata_simbev_run.json file, that is provided along with the trip data.

.. csv-table:: EV types
    :header: "Tecnnology", "Size", "Max. charging capacity slow in kW", "Max. charging capacity fast in kW", "Battery capacity in kWh", "Energy consumption in kWh/km"
    :widths: 10, 10, 30, 30, 25, 10
    :name: ev-types-data

    "BEV", "mini", 11, 120, 60, 0.1397
    "BEV", "medium", 22, 350, 90, 0.1746
    "BEV", "luxury", 50, 350, 110, 0.2096
    "PHEV", "mini", 3.7, 40, 14, 0.1425
    "PHEV", "medium", 11, 40, 20, 0.1782
    "PHEV", "luxury", 11, 120, 30, 0.2138

Heavy-duty transport
+++++++++++++++++++++

In the context of the eGon project, it is assumed that all e-trucks will be
completely hydrogen-powered. The hydrogen demand data of all e-trucks is set up
in the :py:class:`HeavyDutyTransport<egon.data.datasets.emobility.heavy_duty_transport.HeavyDutyTransport>`
dataset for both the eGon2035 and eGon100RE scenario.

In both scenarios the hydrogen consumption is
assumed to be 6.68 kgH2 per 100 km with an additional supply chain leakage rate of 0.5 %
(see `here <https://www.energy.gov/eere/fuelcells/doe-technical-targets-hydrogen-delivery>`_).

For the eGon2035 scenario the ramp-up figures are taken from the
`network development plan (version 2021) <https://www.netzentwicklungsplan.de/sites/default/files/paragraphs-files/NEP_2035_V2021_2_Entwurf_Teil1.pdf>`_
(Scenario C 2035). According to this, 100,000 e-trucks are
expected in Germany in 2035, each covering an average of 100,000 km per year.
In total this means 10 Billion km.

For the eGon100RE scenario it is assumed that the heavy-duty transport is
completely hydrogen-powered. The total freight traffic with 40 Billion km is
taken from the
`BMWK Langfristszenarien <https://www.langfristszenarien.de/enertile-explorer-wAssets/docs/LFS3_Langbericht_Verkehr_final.pdf#page=17>`_
for heavy-duty vehicles larger 12 t allowed total weight (SNF > 12 t zGG).

The total hydrogen demand is spatially distributed on the basis of traffic volume data from [BASt]_.
For this purpose, first a voronoi partition of Germany using the traffic measuring points is created.
Afterwards, the spatial shares of the Voronoi regions in each NUTS3 area are used to allocate
hydrogen demand to the NUTS3 regions and are then aggregated per NUTS3 region.
The refuelling is assumed to take place at a constant rate.
Finally, to
determine the hydrogen bus where the hydrogen demand is allocated to, the centroid
of each NUTS3 region is used to determine the respective hydrogen Voronoi cell (see
:py:class:`GasAreaseGon2035<egon.data.datasets.gas_areas.GasAreaseGon2035>` and
:py:class:`GasAreaseGon100RE<egon.data.datasets.gas_areas.GasAreaseGon100RE>`) it is
located in.

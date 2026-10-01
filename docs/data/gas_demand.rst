The gas demand is modelled with PyPSA *loads* of the carriers
``CH4_for_industry`` and ``H2_for_industry`` in Germany and ``CH4`` abroad.
Gas used for heat supply and power generation is not a load: it is part of the
heat and electricity sectors (e.g. gas boilers, CHP and gas turbines as
*links* from the gas buses).

In Germany
~~~~~~~~~~

The industrial demand of methane and hydrogen over the year comes from the
scenario parameters (``industrial_gas_demand``):

.. list-table:: Industrial gas demand in Germany
   :widths: 16 14 14 56
   :header-rows: 1

   * - Scenario
     - CH4 [TWh]
     - H2 [TWh]
     - Source
   * - status2024
     - 214.7
     - 0
     - AG Energiebilanzen 2024 [AGEB2024]_, Tab. 8: final energy consumption of
       the industry (192.7 TWh, including gas for heat in industrial CHP) plus
       non-energy use (22.0 TWh)
   * - eGon2035
     - 124.9
     - 64.6
     - Szenariorahmen Gas und Wasserstoff 2025, linear between 2030 and 2037
   * - reGon2037
     - 100.9
     - 84.4
     - Szenariorahmen Gas und Wasserstoff 2025, scenario 2
   * - reGon2045
     - 0
     - 202.3
     - Szenariorahmen Gas und Wasserstoff 2025, scenario 2 (no methane demand
       in 2045, biomethane does not have a fixed demand)

The target scenarios follow scenario 2 of the approved Szenariorahmen
[SR_GasH2_2025]_ (based on the long-term scenario O45-Strom: hydrogen mainly in
power plants and industry, aligned with scenarios B/C of the NEP Strom used for
the electricity sector). The approval only gives capacities (GWh/h), no
energies. The energies are therefore those of the same storyline in the draft
of July 2024 ("Fokus Strom", T45-Strom*, numbered scenario 1 there; Tab. 25 and
26), scaled by the ratio of the approved to the draft industry capacity (CH4
2037: 20/23 GWh/h; H2 2037: 18/16 GWh/h, 2045: 42/60 GWh/h), i.e. at the
full-load hours of the draft. The year 2030 used for eGon2035 is not part of the
approved scenario and keeps the draft value.

The temporal distribution comes from the eXtremOS project of the FfE
[eXtremOS]_ (industrial demand of methane and hydrogen in 2035, per NUTS-3
region). It is only used as a pattern: the loads are scaled to the German
totals above. The data is downloaded from the FfE open data platform; as the
platform no longer answers, the copy of the data bundle is used. The status
quo scenario has no hydrogen demand (no hydrogen system).

The spatial distribution differs by carrier:

* *Methane* (all scenarios) and *hydrogen in eGon2035*: the NUTS-3 regions of
  the FfE data, each assigned to the CH4 bus (methane) or the ``H2_grid`` or
  ``H2`` bus (hydrogen) of its Voronoi area. As the ``H2_grid`` buses only
  exist where the core network has a pipeline in the scenario year, hydrogen
  demand far from the network is assigned to an ``H2`` bus at a CH4 node and
  supplied locally (see electrolysis in the gas supply section).
* *Hydrogen in reGon2037 and reGon2045*: the FfE pattern of 2035 does not
  reflect where the much larger hydrogen network of these years supplies
  industry. The German hourly profile (sum of the FfE regions) is therefore
  distributed over the German ``H2_grid`` buses in proportion to the capacity
  of the pipelines connected to each bus (assumption: a proxy for the demand a
  node can serve, not a result of a network simulation). ``H2`` buses get no
  industrial demand in these scenarios.

In a test run with a dataset boundary, the shares are computed for the whole
of Germany and only the buses inside the boundary are kept, so the region
keeps its share of the national total.

The implementation is detailed in :py:mod:`industrial_gas_demand
<egon.data.datasets.industrial_gas_demand>`.

In the neighbouring countries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The target scenarios have one methane load per neighbouring country. It is the
**final demand of methane** of the country (all sectors including households,
services and industry, energy and non-energy use, without power generation,
which is modelled by gas turbines):

* **EU countries**: demand scenarios of the TYNDP 2024 [TYNDP2024_scenarios]_,
  scenario "Distributed Energy", sum of the sector totals of methane. The data
  gives the reference year 2019, 2040 and 2050; the demand is interpolated
  linearly to the year of the scenario.
* **Norway, Switzerland, United Kingdom** (not part of the demand scenarios):
  final consumption of natural gas in 2019 (Eurostat energy balances
  [Eurostat]_ for Norway and the United Kingdom, Swiss overall energy
  statistics [BFE2019]_ for Switzerland), scaled with the development of the
  methane final demand of the EU27 in the same scenario.

.. list-table:: Methane final demand abroad
   :widths: 30 23 23 23
   :header-rows: 1

   * -
     - eGon2035
     - reGon2037
     - reGon2045
   * - Total of the neighbouring countries [TWh]
     - 909
     - 829
     - 556
   * - of which Norway [TWh]
     - 6.4
     - 5.8
     - 3.9

The temporal profile is the rural heat demand of the country in the PyPSA-Eur
run, the largest part of the final demand of methane.

The electricity demand of the electrolysers abroad is a constant electrical
load (carrier ``H2_for_industry``) at the AC bus of the TYNDP node: the yearly
consumption of the electrolysers ("Electrolyser (load)") of the TYNDP 2024
electricity results, scenario "Distributed Energy", interpolated between 2035,
2040 and 2050 (about 44 GW in reGon2037 and 87 GW in reGon2045 on average).

The implementation is detailed in :py:mod:`gas_neighbours.gas_scenarios
<egon.data.datasets.gas_neighbours.gas_scenarios>`.

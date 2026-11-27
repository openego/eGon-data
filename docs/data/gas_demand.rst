The gas demand is modelled with PyPSA *loads* of the carriers
``CH4_for_industry`` and ``H2_for_industry`` in Germany and ``CH4`` abroad.
Gas used for heat supply and power generation is not a load: it is part of the
heat and electricity sectors (e.g. gas boilers, CHP and gas turbines as
*links* from the gas buses).

In Germany
~~~~~~~~~~

The industrial demand of methane and hydrogen over the year is a scenario
parameter (``industrial_gas_demand``), including the non-energy use:

.. list-table:: Industrial gas demand in Germany
   :widths: 16 84
   :header-rows: 1

   * - Scenario
     - Source of the demand over the year
   * - status2024
     - AG Energiebilanzen 2024 [AGEB2024]_, Tab. 8: final energy consumption
       of the industry (including the gas for heat in industrial CHP) plus
       non-energy use; no hydrogen demand (no hydrogen system)
   * - reGon2037
     - Szenariorahmen Gas und Wasserstoff 2025, scenario 2
   * - reGon2045
     - Szenariorahmen Gas und Wasserstoff 2025, scenario 2; no methane demand
       (no methane system in Germany, see the gas grid section)

The target scenarios follow scenario 2 of the approved Szenariorahmen
[SR_GasH2_2025]_ (based on the long-term scenario O45-Strom: hydrogen mainly in
power plants and industry, aligned with scenarios B/C of the NEP Strom used for
the electricity sector). The approval only gives capacities (GWh/h), no
energies. The energies are therefore those of the same storyline in the draft
of July 2024 ("Fokus Strom", T45-Strom*, numbered scenario 1 there; Tab. 25 and
26), scaled by the ratio of the approved to the draft industry capacity, i.e.
at the full-load hours of the draft.

The temporal distribution comes from the eXtremOS project of the FfE
[eXtremOS]_ (industrial demand of methane and hydrogen in 2035, per NUTS-3
region). It is only used as a pattern: the loads are scaled to the German
totals above. The data is downloaded from the FfE open data platform; if the
platform does not answer, the copy of the data bundle is used.

The spatial distribution differs by carrier:

* *Methane*: the NUTS-3 regions of the FfE data, each assigned to the CH4 bus
  of its Voronoi area (in status2024 the only CH4 bus of Germany).
* *Hydrogen* (reGon2037, reGon2045): the same NUTS-3 regions, first rescaled
  per federal state to the industrial hydrogen demand of the market survey of
  the Szenariorahmen [SR_GasH2_2025_Annexes]_ (category "Ausspeisung", exit to
  the industry; 2037: projects with at least the status "Entwurfsplanung",
  2045: all projects; some states are grouped in the survey). Within a state
  the regions keep their shares of the FfE data. The regions are then
  assigned to the ``H2_grid`` or ``H2`` bus of their Voronoi area. Only
  nodes of the node list of the hydrogen network have a Voronoi area:
  ``H2_grid`` buses have a pipeline in the scenario year, ``H2`` buses are
  nodes without one (see the gas grid section; the ``H2`` buses at CH4 buses
  get no demand). Hydrogen demand at an ``H2`` bus is supplied locally (see
  electrolysis in the gas supply section).

The market survey gives values per year until 2035; the shares per federal
state of all its uses in the model (industrial demand, storage, other
imports) are those of 2035.

In a test run with a dataset boundary, the demand is scaled for the whole of
Germany and only the loads inside the boundary are kept, so the region keeps
its share of the national total.

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

The temporal profile is the rural heat demand of the country in the PyPSA-Eur
run, the largest part of the final demand of methane.

The electricity demand of the electrolysers abroad is a constant electrical
load (carrier ``H2_for_industry``) at the AC bus of the TYNDP node: the yearly
consumption of the electrolysers ("Electrolyser (load)") of the TYNDP 2024
electricity results, scenario "Distributed Energy", interpolated between 2035,
2040 and 2050.

The implementation is detailed in :py:mod:`gas_neighbours.gas_scenarios
<egon.data.datasets.gas_neighbours.gas_scenarios>`.

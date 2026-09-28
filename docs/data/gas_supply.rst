Methane production
~~~~~~~~~~~~~~~~~~

Methane is produced by PyPSA *generators* of the carrier ``CH4`` at the CH4
buses. Natural gas and biomethane are the same carrier, distinguished by their
marginal cost; eTraGo limits their production over the year per type with the
parameter ``max_gas_generation_overtheyear``.

**In Germany**

* *Natural gas*: the extraction sites of SciGRID_gas [SciGRID_gas]_. Their
  capacities are a slack, the production over the year is limited by the
  scenario parameter (status2024: 36 TWh, production of 2024; eGon2035:
  9.0 TWh, value of the eGon project; reGon2037: 8.8 TWh, i.e. 1 GWh/h of
  domestic production of the approved Szenariorahmen [SR_GasH2_2025]_;
  reGon2045: none).
* *Biomethane*: the plants feeding into the gas grid of the Einspeiseatlas of
  the dena, version of 9 October 2025 [Einspeiseatlas2025]_ (plants in
  operation and in planning, not the ones out of operation), with their
  feed-in capacity. Their production over the year is limited to 10.4 TWh
  (status2024, BNetzA monitoring report 2025), 10 TWh (eGon2035) and 12.3 TWh
  (reGon2037, reGon2045: feed-in of 2025 as a conservative floor). The fleet
  is the same in all scenarios: the NEP gives no path of the biomethane
  capacity (Tabelle 19 only gives the combined feed-in of production and
  biomethane into the transmission grid, 2 GWh/h in 2037).
* *LNG terminals*: the planned terminals of the approved Szenariorahmen
  [SR_GasH2_2025]_ (clusters Wilhelmshaven 26 GWh/h, Unterelbe with
  Brunsbüttel and Stade 35 GWh/h, Ostsee with Lubmin, Mukran and Rostock
  11.5 GWh/h plus 6 GWh/h already built at Mukran; shared equally within a
  cluster). They feed in with the share of their capacity that the NEP uses in
  its peak load case [NEP_GasH2_2025]_: 50 % in 2037 (scenario 2, 39 GWh/h in
  Tabelle 19), all of it in 2030 (scenario 4), linear in between (64 % in
  eGon2035), none in 2045 (the LNG permits end in 2043). Their price is the LNG
  price of the neighbouring countries (natural gas plus 30 %), so they are
  used when the pipelines are full, and they are not limited over the year.
* *Status quo*: the only CH4 bus of Germany gets one slack generator at the
  marginal cost of natural gas, bounded by the production over the year.

The marginal cost of natural gas includes the CO2 costs; biomethane has its own
cost (technology data or dena market prices). In status2024, reGon2037 and
reGon2045 biomethane is more expensive than natural gas and imports (e.g.
reGon2037: 59 vs. 47.6 EUR/MWh), so it is dispatched last; in eGon2035 it is
cheaper (25.6 vs. 41.0 EUR/MWh).

The implementation is detailed in :py:mod:`ch4_prod <egon.data.datasets.ch4_prod>`.

**In the neighbouring countries**, the supply of methane per country comes
from the TYNDP 2024 of ENTSOG [TYNDP2024_gas]_:

* Annex E "Analysis tables", sheet "Supply": national production
  (conventional, biomethane, synthetic methane) and imports from outside the
  modelled area (e.g. Norway), scenario "NT+ Reference" (the scenario
  "Distributed Energy" was not simulated in Annex E), infrastructure level
  "ADVANCED"; peak and mean of the monthly values give the capacity and the
  production over the year, interpolated to the year of the scenario.
  Synthetic methane is priced like natural gas.
* Annex C1, sheet "LNG": LNG import capacities, level "ADVANCED".

The sources are blended into one marginal cost per country. In reGon2045 only
biomethane is produced abroad, RU excluded.

Methanation and SMR
~~~~~~~~~~~~~~~~~~~

Methanation (``H2_to_CH4``) and steam methane reforming (``CH4_to_H2``) are
extendable unidirectional *links* between each ``H2_grid`` or ``H2`` bus and
the nearest CH4 bus within 10 km. Efficiency, costs and lifetime come from the
technology data [technoData]_. The status quo scenario has no hydrogen system.
There is no feed-in of hydrogen into the methane grid: the NEP Gas und
Wasserstoff 2025 plans methane and hydrogen as separate networks without
blending.

The implementation is detailed in :py:mod:`h2_to_ch4 <egon.data.datasets.hydrogen_etrago.h2_to_ch4>`.

Electrolysis, fuel cells and their co-products
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Electrolysis** (``power_to_H2``) is an extendable *link* from an AC bus
(substation of the HV/MV or EHV level) to an ``H2_grid`` or ``H2`` bus. The
candidates are the substations within 5 km (HV/MV) or 20 km (EHV) of a
pipeline or an ``H2`` bus. The capacity of all electrolysers of a federal
state is limited to the capacity of the NEP Strom (eGon2035: 8.5 GW,
NEP 2035 [NEP2021]_; reGon2037: 40.7 GW and reGon2045: 68.8 GW, scenario C of
the NEP 2037/2045 [NEP2025]_). The electricity side (scenario C) sets the
electrolysers and the gas side follows it: scenario 2 of the NEP Gas und
Wasserstoff 2025 [NEP_GasH2_2025]_ assumes 42 GW (2037) and 58 GW (2045), in
line with scenario B of the NEP Strom, so reGon2045 has about 11 GW more
domestic electrolysis than the gas NEP. Within a state it is distributed over 
the links in proportion to their connection level (120 MW at HV/MV, 5000 MW 
at EHV substations). If the substations of a state can not host its capacity, 
this is reported and the state keeps less. The efficiency (LHV) comes from the 
NEP (0.70 in 2037) and the literature, the costs from the technology data of 
the Danish Energy Agency for renewable fuels [DEA_RF]_.

H2 buses with industrial hydrogen demand that the core network does not reach
(``H2`` buses at CH4 nodes, only in eGon2035, see the gas demand section) and
that have no electrolyser candidate get an electrolyser at the nearest substation. 
It counts towards the NEP capacity of the federal state; if the state capacity is 
too small, the demand at such a bus may not be met.

**Fuel cells** (``H2_to_power``) are extendable links in the opposite
direction at the same locations.

**Co-products.** The waste heat of the electrolysers can be used in district
heating (``PtH2_waste_heat``, links from the AC bus to the nearest district
heating bus). Its capacity is bounded by what the electrolysers at the same
AC bus can produce: 5.7-5.8 % of the electrical input as recoverable heat at
70 °C [DEA_RF]_. **Limitation:** the co-product link starts at the AC bus, so
its operation is not coupled hour by hour to the operation of the
electrolysers; this would need links with several outputs in eTraGo. The
oxygen of the electrolysers is not modeled.

The implementation is detailed in 
:py:mod:`power_to_h2 <egon.data.datasets.hydrogen_etrago.power_to_h2>`.

Gas-fired and hydrogen power plants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Power plants burning gas are *links* from the gas bus to the AC bus
(carrier ``OCGT``); combined heat and power plants are modelled by the CHP
datasets. Their variable O&M costs are given per MWh of electricity and are
applied per MWh of fuel multiplied by the efficiency, as in PyPSA-Eur.

In the reGon scenarios, the plants follow the approved list of power plants of
the Szenariorahmen 2025 (Annex 1 of the approval [SR_Strom_2025]_), which
separates natural gas units (2037 only) from hydrogen power plant projects:

* *Natural gas plants* (reGon2037): the units of the list, located with their
  MaStR number, link from the nearest CH4 bus, efficiency of an open cycle gas
  turbine (0.42). The reserve plants of the list (grid reserve, capacity
  reserve and the "besondere netztechnische Betriebsmittel", 19 units,
  2.4 GW, status of the Kraftwerksliste of the Bundesnetzagentur [BNetzA_KWL]_)
  are left out, as the NEP Strom does not use them in its market model
  ([NEP2025]_ Kap. 2, p. 29); per federal state the capacity then matches the
  NEP Strom. Their sites count as freed connections for new hydrogen plants,
  as the StromVKG allows new builds at sites of the grid and capacity reserve.
* *Hydrogen power plants* (reGon2037, reGon2045): the capacity per federal
  state of the NEP Strom 2037/2045 [NEP2025]_ (40.4 GW in 2037, 81.3 GW in
  2045). The projects of the list are only known by federal state. They are
  placed at existing sites so that the conversion of the natural gas plants
  can be followed site by site:

  - *Conversions*: the market survey of the Szenariorahmen Gas/Wasserstoff
    2025 [SR_GasH2_2025_Annexes]_ (Annex 2) flags the projects that reduce the
    methane demand of their site (206 projects, 22 GW in 2037), its list of
    gas power plants (Annex 3) the natural gas units whose site was reported
    in the survey. Most of these units are no longer in the list in 2037
    (17 GW), only 0.6 GW of natural gas units leave the list without such a
    report. As the survey keeps the projects anonymous, each conversion
    project is paired with the reported site of its federal state that fits
    best in capacity and timing, if the capacities differ by less than a
    factor of 4 (154 projects, median ratio of the capacities 1.05): the
    hydrogen plant takes the place of the natural gas units at the same site.
  - *New builds* (and conversions without a reported site): at the freed
    connections of the plants that the NEP no longer has in 2037, i.e. the
    natural gas capacity of a site in the MaStR that is not in the list in
    2037 and the coal capacity (the NEP has no coal plants in 2037), minus
    the conversions at the site. The grid connection of these sites exists
    and is free again; operators plan most of their new H2-ready plants at
    such sites (e.g. Weisweiler, Schwarze Pumpe, Lippendorf, Staudinger,
    Scholven). The projects are first paired with the freed connections by
    capacity, the rest goes to the largest remaining freed connection,
    preferably at a site without a hydrogen plant yet. Decommissioned MaStR
    units without location (e.g. Neurath) are assigned to the nearest site
    within 1 km or form a site of their own. In 2037, 23.8 GW are placed at
    freed connections (21.0 GW on coal sites) and 0.9 GW at other existing
    sites; 2.3 GW stand next to a natural gas plant that still runs.
  - *Further plants in 2045*: the list has no natural gas plants in 2045, so
    the natural gas sites of 2037 are converted first, up to their capacity
    in 2037 (reported sites first); the rest is placed load-near at existing
    power plant sites, in proportion to the population of their EHV
    substation area.

  In 2037 the listed projects are scaled to the capacity of their federal
  state; in 2045 they keep their capacity (scaled down only if they exceed
  it) and the rest is placed load-near. Sites are MaStR locations with
  combustion units; new builds and load-near plants only use sites of at
  least 10 MW. In the pairing of the conversions, a project that starts in
  another year than the natural gas units of the site leave the list costs
  as much as a factor of 2 in capacity (assumption).

  A project is at the same site in 2037 and 2045. The sources of each plant
  give the siting ("listed" or "load-near") and the choice of the site
  ("conversion", "freed connection", "existing site", "gas site 2037" or
  "population"), its source_id the units it replaces. The
  plants are links from the nearest ``H2_grid`` or ``H2`` bus.
  As the NEP models them as peak-load plants with a lower efficiency for the
  load-near plants, the plants of the list get the efficiency of a combined
  cycle plant (0.58 in 2037, 0.60 in 2045) and the load-near plants the one
  of a gas turbine (0.42), from the technology data.

  The NEP C counts 41 GW as hydrogen plants in 2037. The Kraftwerksstrategie
  of the federal government [StromVKG]_ requires the new plants to be H2-ready
  and climate-neutral from 2045, but expects only 4 GW to switch to hydrogen
  before 2045 (2 GW by 2040, 2 GW by 2043). The reGon scenarios follow the
  NEP; reGon2037 is therefore more ambitious than the strategy, reGon2045 is
  consistent with it.

**Abroad**, the gas-fired power plants are OCGT links from the CH4 bus to the
AC bus of the TYNDP node: installed capacity of the gas technologies of the
TYNDP 2024 electricity results, scenario "Distributed Energy"
[TYNDP2024_scenarios]_, interpolated between 2035, 2040 and 2050.

The implementation is detailed in :py:mod:`power_etrago.match_ocgt
<egon.data.datasets.power_etrago.match_ocgt>`, :py:mod:`power_plants.hydrogen
<egon.data.datasets.power_plants.hydrogen>` and :py:mod:`gas_neighbours.gas_scenarios
<egon.data.datasets.gas_neighbours.gas_scenarios>`.

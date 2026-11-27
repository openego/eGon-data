Methane production
~~~~~~~~~~~~~~~~~~

Methane is produced by PyPSA *generators* of the carrier ``CH4`` at the CH4
buses. Natural gas and biomethane are the same carrier, distinguished by their
marginal cost; eTraGo limits their production over the year per type with the
gas parameter ``max_gas_generation_overtheyear``.

**In Germany**

* *Natural gas*: the extraction sites of SciGRID_gas [SciGRID_gas]_. The
  production over the year is the scenario parameter: the domestic production
  of 2024 in status2024, the domestic production of the approved Szenariorahmen
  of the NEP Gas und Wasserstoff 2025 [SR_GasH2_2025]_ in reGon2037, and none
  in reGon2045 (phase-out of natural gas). This production at full load is the
  capacity, shared over the sites by their maximal supply in SciGRID_gas.
* *Biomethane*: the plants feeding into the gas grid of the Einspeiseatlas of
  the dena, version of 9 October 2025 [Einspeiseatlas2025]_ (plants in
  operation and in planning, not the ones out of operation), with their
  feed-in capacity. The fleet is the same in all scenarios: the NEP Gas und
  Wasserstoff gives no path of the biomethane capacity (Tabelle 19 only gives
  the combined feed-in of production and biomethane into the transmission
  grid). The production over the year is the feed-in of 2024 (BNetzA
  monitoring report 2025) in status2024 and, as a conservative floor, the
  feed-in of 2025 in reGon2037. reGon2045 has no methane system in Germany
  (see the gas grid section), so no natural gas, biomethane or LNG supply.
* *LNG terminals*: the planned terminals of the approved Szenariorahmen of the
  NEP Gas und Wasserstoff 2025 [SR_GasH2_2025]_ (capacity per cluster
  Wilhelmshaven, Unterelbe and Ostsee, shared equally by its terminals). They
  feed in with the share of their capacity that the NEP Gas und Wasserstoff
  uses in its peak load case [NEP_GasH2_2025]_ (Tabelle 19): half of it in 2037
  (scenario 2), all of it in 2030 (scenario 4), linear in between, and none in
  2045 (the LNG permits end in 2043). Their price is the LNG price of the
  neighbouring countries (natural gas plus 30 %), so they are used when the
  pipelines are full, and they are not limited over the year.
* *Status quo*: the only CH4 bus of Germany gets one slack generator at the
  marginal cost of natural gas, bounded by the production over the year.

The marginal cost of natural gas is the gas price plus the CO2 costs, as the
price formula of the NEP Strom [NEP2025]_ (Tabelle 9). Biomethane has its own
cost (dena market prices in status2024, technology data in the target
scenarios); it is more expensive than natural gas and the imports, so it is
dispatched last.

The implementation is detailed in :py:mod:`ch4_prod <egon.data.datasets.ch4_prod>`.

**In the neighbouring countries**, the supply of methane per country comes
from the TYNDP 2024 of ENTSOG [TYNDP2024_gas]_:

* Annex E "Analysis tables", sheet "Supply": national production
  (conventional, biomethane, synthetic methane) and imports from outside the
  modelled area (e.g. Norway), scenario "NT+ Reference" (the scenario
  "Distributed Energy" was not simulated in Annex E), infrastructure level
  "ADVANCED"; peak and mean of the monthly values give the capacity and the
  production over the year, interpolated to the year of the scenario
  (extrapolated from 2030 and 2040 for 2045, not below zero). Synthetic
  methane is priced like natural gas.
* Annex C1, sheet "LNG": LNG import capacities, level "ADVANCED".

The sources are blended into one marginal cost per country, RU excluded.

Hydrogen imports
~~~~~~~~~~~~~~~~

The hydrogen buses abroad (see the gas grid section) have no supply of their
own, so the imports are modeled as ``H2`` generators with a fixed capacity:

* *Border points*: at the bus of each country, with the entry capacity of its
  border points in scenario 2 of the NEP Gas und Wasserstoff 2025
  [NEP_GasH2_2025]_.
* *Converted LNG terminals* (reGon2045): at the ``H2_grid`` buses of
  Wilhelmshaven, Stade and Brunsbüttel, with their entry capacity of
  Tabelle 36.
* *Other imports* (LH2 and derivatives, mostly ammonia; Tabellen 21 and 35):
  shared over the federal states like the import projects of the market
  survey of the Szenariorahmen [SR_GasH2_2025_Annexes]_ (projects with at
  least the status "Entwurfsplanung" in 2037, all projects in 2045) and placed
  at the nearest ``H2_grid`` bus of one site per state: the LNG terminals in
  the coastal states, as the approval of the Szenariorahmen asks
  [SR_GasH2_2025]_ (Wilhelmshaven and Stade, Brunsbüttel, Rostock and
  Lubmin), elsewhere the largest chemical site (Marl, Leuna, Schwedt, Böhlen,
  Burghausen; assumption). States without such a site are left out. The sites
  are located with the node list of the hydrogen grid (see the gas grid
  section), as are the LNG terminals for methane.

The price of the imports is the hydrogen price of the NEP Strom, which is
derived from the natural gas price and the CO2 cost ([NEP2025]_ Tabelle 9).
The approval of the Szenariorahmen and the NEP only give capacities, so the
imports over the year are limited to the hydrogen imports of the draft
Szenariorahmen of July 2024 ("Fokus Strom", T45-Strom*, numbered scenario 1
there; Tab. 3). As for the methane supply abroad, the limit is the
``e_nom_max`` of each generator: the total (gas parameter
``max_H2_import_overtheyear``) is shared over the generators by their
capacity. The domestic production of hydrogen is modeled by links, so the H2
generators are exactly the imports.

The implementation is detailed in :py:func:`insert_h2_imports
<egon.data.datasets.hydrogen_etrago.h2_grid.insert_h2_imports>`.

Methanation and SMR
~~~~~~~~~~~~~~~~~~~

Methanation (``H2_to_CH4``) and steam methane reforming (``CH4_to_H2``) are
extendable unidirectional *links* between each ``H2_grid`` or ``H2`` bus and
the nearest CH4 bus within 10 km. Efficiency, costs and lifetime come from the
technology data [technoData]_. The status quo scenario has no hydrogen system,
reGon2045 no CH4 bus in Germany, so neither has these links.
There is no feed-in of hydrogen into the methane grid: the NEP Gas und
Wasserstoff 2025 plans methane and hydrogen as separate networks without
blending.

The implementation is detailed in :py:mod:`h2_to_ch4 <egon.data.datasets.hydrogen_etrago.h2_to_ch4>`.

Electrolysis, fuel cells and their co-products
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Electrolysis** (``power_to_H2``) is an extendable *link* from an AC bus
(substation of the HV/MV or EHV level) to an ``H2_grid`` or ``H2`` bus. The
candidates are the substations near a pipeline or an ``H2`` bus (buffers of
10 km around HV/MV and 20 km around EHV substations that overlap the buffer of
5 km or 20 km around the pipeline or bus); a substation is linked to the
nearer end of the closest pipeline. Each link is limited by its connection
level (120 MW at HV/MV, 5000 MW at EHV substations), so the optimisation
decides where the electrolysers are built.

The capacity of the NEP Strom per federal state (row "zusaetzliche
Verbraucher PtG" of the NEP files of the data bundle; scenario C of the NEP
Strom 2037/2045 [NEP2025]_) is stored with the other capacities of the NEP in
``supply.egon_scenario_capacities`` (carrier ``power_to_H2``), for a
constraint per federal state in eTraGo. The electricity side (scenario C)
sets the electrolysers and the gas side follows it: scenario 2 of the NEP Gas
und Wasserstoff 2025 [NEP_GasH2_2025]_ assumes other capacities, in line with
scenario B of the NEP Strom. The efficiency (lower heating value) comes from
the NEP Strom for 2037 and from the literature for 2045, the costs from the
technology data of the Danish Energy Agency for renewable fuels [DEA_RF]_.

``H2`` buses with industrial hydrogen demand that no pipeline reaches and that
have no electrolyser candidate (see the gas demand section) get an
electrolyser at the nearest substation. It counts towards the NEP Strom
capacity of the federal state.

**Fuel cells** (``H2_to_power``) are extendable links in the opposite
direction at the same locations, with the same connection limits.

**Co-products.** The waste heat of the electrolysers can be used in district
heating (``PtH2_waste_heat``, links from the AC bus to the nearest district
heating bus). Its capacity is bounded by what the electrolysers at the same AC
bus can produce: the share of the electrical input that is recoverable as
heat at 70 °C [DEA_RF]_. **Limitation:** the co-product link starts at the AC
bus, so its operation is not coupled hour by hour to the operation of the
electrolysers; this would need links with several outputs in eTraGo. The
oxygen of the electrolysers is not modeled.

The implementation is detailed in
:py:mod:`power_to_h2 <egon.data.datasets.hydrogen_etrago.power_to_h2>`.

Gas-fired power plants
~~~~~~~~~~~~~~~~~~~~~~

Power plants burning gas are *links* from a gas bus to an AC bus. Gas turbines
have the carrier ``OCGT``; combined heat and power plants are modelled by the
CHP datasets. The variable O&M costs are given per MWh of electricity and are
applied per MWh of fuel multiplied by the efficiency, as in PyPSA-Eur.

**Natural gas plants in Germany (reGon).** The natural gas plants are the
units of the power plant list of the scenario capacities, as for the other
conventional plants. They are linked from the nearest CH4 bus with the
efficiency of an open cycle gas turbine from the technology data.

The power plant list of the scenario capacities is the list of the draft of
the Szenariorahmen of the NEP Strom. Its approval replaced it by Annex 1, the
power plant list of the NEP Strom 2037/2045 (2025) [SR_Strom_2025]_ (the same
list is Annex 1 of the approval of the Szenariorahmen of the NEP Gas und
Wasserstoff 2025 [SR_GasH2_2025]_), which separates natural gas units (2037
only) from hydrogen power plant projects. The hydrogen plants are sited with
Annex 1 (see below), so the natural gas units of the draft are aligned with it
by MaStR number:

* natural gas units get the capacities of Annex 1, so scenario C has no
  natural gas plants in 2045, as in the NEP Strom;
* natural gas units of the draft that are not in Annex 1 are removed, units
  only in Annex 1 are added with the data of the MaStR, and units listed twice
  in the draft are kept once;
* the steam turbines of combined cycle plants, which Annex 1 lists as other
  power plants ("Dampf"), keep their capacity in both years;
* the new builds of Annex 1 (§§ 38/39 GasNZV) have no MaStR number and no
  location: they get the postcode and municipality of their names in the code
  and are placed like the planned plants of the draft (matched to a MaStR unit
  of the same postcode or municipality);
* the row of Annex 1 for the units below 10 MW is not added, as the small CHP
  plants cover these units.

This alignment only affects the natural gas units; the other rows of the
draft are unchanged. The reserve plants (grid reserve, capacity reserve and
the "besondere netztechnische Betriebsmittel", status of the Kraftwerksliste
of the Bundesnetzagentur [BNetzA_KWL]_) have no capacity in 2037, as the NEP
Strom does not use them in its market model ([NEP2025]_ Kap. 2, p. 29).

**Abroad**, the gas-fired power plants are OCGT links from the CH4 bus to the
AC bus of the TYNDP node: installed capacity of the gas technologies of the
TYNDP 2024 electricity results, scenario "Distributed Energy"
[TYNDP2024_scenarios]_, interpolated between 2035, 2040 and 2050.

The implementation is detailed in :py:mod:`power_etrago.match_ocgt
<egon.data.datasets.power_etrago.match_ocgt>`, :py:func:`align_nep2025_gas_units
<egon.data.datasets.power_plants.hydrogen.align_nep2025_gas_units>` and
:py:mod:`gas_neighbours.gas_scenarios
<egon.data.datasets.gas_neighbours.gas_scenarios>`.

Hydrogen power plants
~~~~~~~~~~~~~~~~~~~~~

The reGon scenarios have the hydrogen power plant capacity per federal state
of the NEP Strom 2037/2045 [NEP2025]_ (scenario C, row "Wasserstoff" of the
NEP files of the data bundle). As in the NEP Strom, the hydrogen plants are
peak-load plants that only burn hydrogen: natural gas and hydrogen plants are
separate plants (no blending, no dual-fuel operation). Only the conversions of
natural gas CHP units produce heat (see below).

The projects of Annex 1 [SR_Strom_2025]_ are only known by federal state.
They are placed at existing sites, so that the conversion of the natural gas
plants can be followed site by site. Sites are MaStR locations with combustion
units.

* *Conversions*: the hydrogen market survey of the draft Szenariorahmen of
  the NEP Gas und Wasserstoff 2025 [SR_GasH2_2025_Annexes]_ (Annex 2) flags
  the projects that reduce the methane demand of their site; the list of gas
  power plants of the same draft (Annex 3), which the BNetzA used for the
  power plant list ([SR_GasH2_2025]_ p. 34), flags the natural gas units whose
  site was reported in the survey. As the survey keeps the projects
  anonymous, each conversion project is paired with the reported site of its
  federal state that fits best in capacity and timing: the hydrogen plant
  takes the place of the natural gas units at the same site.
* *New builds* (and conversions without a reported site): at the freed
  connections of the plants that the NEP Strom no longer has in 2037, i.e. the
  natural gas capacity of a site in the MaStR that is not in the list in 2037
  and the coal capacity (the NEP Strom has no coal plants in 2037), minus the
  conversions at the site. The grid connection of these sites exists and is
  free again; operators plan most of their new H2-ready plants at such sites
  (e.g. Weisweiler, Schwarze Pumpe, Lippendorf, Staudinger, Scholven). The
  sites of the reserve plants count as freed connections, as the StromVKG
  [StromVKG]_ allows new builds at sites of the grid and capacity reserve. The
  projects are first paired with the freed connections by capacity; the rest
  goes to the largest remaining freed connection, preferably at a site
  without a hydrogen plant yet, or else to the existing site with the largest
  remaining capacity.
* *Load-near plants*: egon-data only gives the sites of the capacity of a
  federal state beyond the list (in 2045, or in a federal state without
  listed projects); eTraGo sizes the plants (see below). The candidates are
  the existing power plant sites of the federal state and, in 2045, the
  natural gas sites of 2037, as the list has no natural gas plants in 2045.

**Assumptions of the siting.**

* In 2037 the listed projects are scaled to the capacity of their federal
  state; in 2045 they keep their capacity (scaled down only if they exceed it)
  and the rest goes to load-near plants.
* A project is at the same site in 2037 and 2045; a project of 2045 already
  takes its site in 2037.
* New builds and load-near plants only use sites of at least 10 MW. Sites
  whose coordinates lie in another federal state than their MaStR entry are
  not used (wrong coordinates). Decommissioned MaStR units without location
  are assigned to the nearest site within 1 km or form a site of their own.
* The pairings (linear sum assignment) keep the capacities of project and
  site as close as possible, at most a factor of 4 apart. In the pairing of
  the conversions, a project of 2037 fits a site whose natural gas units leave
  the list by 2037, a project of 2045 a site whose units are still there in
  2037; a mismatch costs as much as a factor of 2 in capacity (assumption).

**Representation.** The plants are links from the nearest ``H2_grid`` or
``H2`` bus to the AC bus, with a carrier per type so that they can be traced
in eTraGo: ``H2_OCGT_conversion`` (conversion of a gas site), ``H2_OCGT_new``
(new build at a freed connection or an existing site) and
``H2_OCGT_load_near``. In ``supply.egon_power_plants``, the sources of each
plant give the siting ("listed" or "load-near") and the choice of the site
("conversion", "freed connection", "existing site" or "gas site 2037"), its
source_id the units it replaces. The listed plants have a fixed capacity. As
for the electrolysers, the load-near plants are extendable links from zero
capacity (zero capacity in ``supply.egon_power_plants``) without an upper
limit, so that eTraGo decides where they are built. They are connected at the
voltage level that follows the MaStR net capacity of the site and have the
investment cost and lifetime of a gas turbine of the technology data. The NEP capacity per
federal state is stored in ``supply.egon_scenario_capacities`` (carrier
``hydrogen``) for a constraint per federal state in eTraGo: the hydrogen
plants of a state together reach at least this electrical capacity. As the
NEP Strom gives the load-near plants a lower efficiency, the plants of the
list get the efficiency of a combined cycle plant and the load-near plants the
one of a gas turbine, both from the technology data.

**Combined heat and power.** A conversion keeps the heat extraction of the
natural gas units it replaces. As in the CHP datasets, the CHP units are the
units with a thermal capacity in the MaStR; they have the ratio of thermal to
electrical capacity of their CHP system (the MaStR gives these capacities per
CHP system, not per unit). As a simplification, the hydrogen plant keeps the
mean ratio of the replaced units, weighted by their net capacity, so units
without heat extraction count as zero; its heat capacity is this ratio times
its electrical capacity. As for the natural gas CHP plants (see the CHP
datasets), a plant close to a district heating area and not at an industrial
site supplies the nearest district heating area: a link
``central_H2_CHP_heat`` from the hydrogen bus of the plant to the central heat
bus of the area, with the heat
efficiency of the gas CHP (electrical efficiency divided by the c_v
coefficient of the technology data). The other conversions produce power
only. **Limitation:** the heat supply of the district heating areas only
counts the CHP plants of the CHP datasets, so the heat of these plants comes
on top of it.

The reGon scenarios follow the NEP Strom. Its hydrogen capacity in 2037 is
ambitious compared to the Kraftwerksstrategie of the federal government
[StromVKG]_, whose new plants have to be H2-ready and switch to hydrogen by
2045 at the latest.

The implementation is detailed in :py:mod:`power_plants.hydrogen
<egon.data.datasets.power_plants.hydrogen>` and :py:mod:`power_etrago.match_ocgt
<egon.data.datasets.power_etrago.match_ocgt>`.

In the gas sector, only the transport grids are represented: the methane
(CH4) transmission grid and the hydrogen (H2) core network. Both are modeled
as PyPSA *links* between *buses* of the carriers ``CH4``, ``H2_grid`` and
``H2``. The model follows the official network planning: the Netzentwicklungsplan
Gas und Wasserstoff 2025 [NEP_GasH2_2025]_ has priority, the approved hydrogen
core network (Wasserstoff-Kernnetz) of the FNB Gas [Kernnetz]_ is refined by it.

Scenarios
~~~~~~~~~

The gas sector is modeled for four scenarios: status2024, eGon2035, reGon2037
and reGon2045. The status quo scenario represents the actual system with a reduced
depth, the target scenarios follow the national planning documents with the same
methods:

.. list-table:: Gas grids per scenario
   :widths: 16 28 28 28
   :header-rows: 1

   * - Scenario
     - CH4 grid in Germany
     - H2 grid in Germany
     - Gas grids abroad
   * - status2024
     - one CH4 bus for Germany (no pipelines)
     - none
     - none
   * - eGon2035
     - SciGRID_gas, without the pipelines converted to H2 until 2035
     - Kernnetz and NEP measures commissioned until 2035
     - one CH4 bus per country, TYNDP 2024
   * - reGon2037
     - SciGRID_gas, without the pipelines converted to H2 until 2037
     - Kernnetz and NEP measures commissioned until 2037
     - one CH4 bus per country, TYNDP 2024
   * - reGon2045
     - methane network 2045 of the NEP (about 5500 km)
     - Kernnetz and additional sections 2045 of the NEP
     - one CH4 bus per country, TYNDP 2024

The implementation is detailed in :py:mod:`gas_grid <egon.data.datasets.gas_grid>`,
:py:mod:`hydrogen_etrago.bus <egon.data.datasets.hydrogen_etrago.bus>`,
:py:mod:`hydrogen_etrago.h2_grid <egon.data.datasets.hydrogen_etrago.h2_grid>`,
:py:mod:`hydrogen_etrago.nep2025 <egon.data.datasets.hydrogen_etrago.nep2025>`
and :py:mod:`gas_neighbours <egon.data.datasets.gas_neighbours>`.

Methane grid
~~~~~~~~~~~~

The methane grid is based on the SciGRID_gas data model [SciGRID_gas]_ (IGGIELGN
data set). The nodes of the German grid are CH4 buses, the pipelines are
bidirectional links. Their capacity is derived from the diameter of the
pipelines with the classification of [Kunz]_. The length of the links is
given in km.

**Status quo (status2024).** Germany is one CH4 bus without pipelines. It is
supplied by a slack generator bounded by the domestic production over the year
(see the gas supply section).

**Pipelines converted to hydrogen (eGon2035, reGon2037).** A conversion of
the Kernnetz lists of the FNB Gas removes methane pipelines from the year it
is commissioned (NEP Anlage 1b where given, otherwise the list): 133
conversions (4807 km) by 2035 and 135 (4860 km) by 2037. The lists have no
identifier of the SciGRID_gas pipelines. For every converted pipeline the CH4
pipelines along the straight line between its end points are flagged
(corridor of 8 km, at least 30 % of the length of the CH4 pipeline inside,
same diameter class first), until the length of the converted pipeline is
reached (at most 1.3 times). The flagged pipelines are removed, longest first,
unless the removal would cut a German bus off from every border point or
remove its last pipeline; these pipelines are kept (about 1150 km in reGon2037).
The pipelines of the methane network 2045 of the NEP (see below) are not
candidates: they remain methane pipelines, so they can not be the converted
ones, and the grid of the earlier scenarios stays consistent with the one of
2045. The CH4 buses are not changed.

**Methane network 2045 (reGon2045).** The NEP lists the methane pipelines that
remain in 2045 (Anhang 5, 74 sections, 5531 km). Each section is matched to
the shortest path of SciGRID_gas pipelines between CH4 buses within 15 km of
its end points, choosing the path whose length fits the NEP length best. All
other German pipelines are removed. CH4 buses without pipeline remain: the NEP
assumes biomethane to be used mainly at the distribution level, so these buses
keep their biomethane plants and local demand.

Not modeled: the methane expansion measures of the NEP up to 2037 (mostly
pressure regulation and metering stations, reconnections and system
separations, below the resolution of SciGRID_gas). The hydrogen measures of
the NEP beyond the Kernnetz (see below) remove no CH4 pipelines before 2045:
they are local pipelines or not identifiable in SciGRID_gas.

Hydrogen grid
~~~~~~~~~~~~~

**Hydrogen core network (all target scenarios).** The pipelines come from the
lists of the Kernnetz of the FNB Gas of 10 December 2024 [Kernnetz]_ (new
builds, conversions of CH4 pipelines and pipelines of further operators,
e.g. Gasnetz Hamburg). The NEP drops three Kernnetz measures (KLN001-01,
KLN025-01, KLU045-01, Anhang 2) but replaces each of them by another
measure on nearly the same route (e.g. H2-201 Überackern-Haiming for
KLN001-01), so they are kept as proxies of their replacements. The
commissioning year of the NEP (Anlage 1b) replaces
the one of the list where the NEP gives it. A pipeline is only part of a
scenario if it is commissioned by the year of the scenario. The costs come
from the investment of the list or, if not given, from the specific costs of
a new H2 pipeline. The pipelines are not extendable.

The capacity of a pipeline follows from its nominal diameter (DN) and design
pressure (DP), scaled from the design capacities of the hydraulic simulations
of the European gas TSOs for the European Hydrogen Backbone [EHB2021]_: 13 GW
for DN 1200 at 80 bar and 1.2 GW for DN 500 at 50 bar, linear in the pressure
and with a power of 2.18 of the diameter fitted to these two points (4.3 GW
for DN 900 at 50 bar, EHB: 3.6-4.7 GW). Examples: DN 1400 / 100 bar 22.8 GW,
DN 1000 / 84 bar 9.2 GW, DN 600 / 70 bar 2.5 GW, DN 400 / 70 bar 1.0 GW.

The end points of the pipelines are named places, located with the node list
of the data bundle and three nodes missing in it (Wiefelstede, Niederaußem,
Coesfeld; coordinates of the municipality). Only nodes with a pipeline in the
scenario year become ``H2_grid`` buses; the node list contains places of an
older version of the lists that no pipeline uses any more. Places with two
names but the same coordinates (e.g. "Hittistetten" and "Hittistetten
(Senden)") are one bus. Some pipelines are split at intermediate nodes
following the detailed map of the FNB Gas; length and investment of a split
pipeline are shared between its parts in proportion to their straight
distance.

Parts of the core network that no pipeline of the lists connects to the rest
(the lists name the places coarsely, e.g. Hamburg Süd/Mitte/Ost, Moosburg,
Amelsbüren/Rinkerode/Uentrop, Perl/Besch) are approved pipelines as well. Each
of them is connected by a straight pipeline from its bus closest to the rest
of the network to the nearest bus there, with the capacity of its largest
pipeline and the commissioning year of its first one (assumption). Parts
connected to a neighbouring country (e.g. Freiburg via Fessenheim) keep that
connection only. The same applies to measures of the NEP whose connection to
the network the NEP does not name (e.g. Dernbach-Bendorf, Weißenhorn); in
reGon2037 eleven (227 km) and in reGon2045 twelve (157 km) such connections of
5-44 km remain (accepted as assumptions).

**Known limitation.** Pipelines whose start and end are the same named place
can not be placed and are not inserted: 57 km with the lists of 2024, mostly
the pipelines of Gasnetz Hamburg within "Hamburg Süd" (38 km) and "Hamburg
Mitte" (11 km). The Hamburg network is represented by the pipelines Hamburg
Süd - Mitte - Ost and its connection to the rest of the network.

**Hydrogen measures of the NEP up to 2037.** The NEP adds measures beyond
the Kernnetz (Anlage 3 of [NEP_GasH2_2025]_). Used are the ones of the
modeling result 2037 of scenario 2, on which the network 2045 of the NEP
builds: the network proposal (criteria H2(4) and H2(6)) and the measures of
scenario 2 that are not part of the proposal (criterion H2(5)), 47 measures.
A measure is part of a scenario from its commissioning year: the date of the
consultation list of the Bundesnetzagentur [BNetzA_IBN2026]_ where given (21
of the 47 measures), otherwise 12/2036 as stated by the NEP. Only H2-233
(7 km, 2031) is commissioned before 2035, so eGon2035 has 1 measure,
reGon2037 39 and reGon2045 40; the others end within one node. The NEP gives
neither length nor DN/DP of these measures: the length is taken from
Tabelle 29 of the NEP where given, for H2-233 (the former Kernnetz measure
KLU121-01) from the FNB Gas application list of 22 July 2024, and otherwise
estimated from the straight line (times 1.163, the median of the sections of
Anhang 6); DN/DP come from the SciGRID_gas pipelines along the route for 10
long measures, otherwise from the median of the conversions of Anhang 6
(DN 600 / 70 bar from 15 km, DN 400 / 70 bar below). Measures within one place
and "Isarschiene Ost/West" (no end points published) are not placed.

**Hydrogen network 2045 (reGon2045).** In addition, the sections of the NEP
beyond the modeling result 2037 are inserted (Anhang 6: 193 sections,
7912 km in all scenarios; the 191 sections / 7857 km of scenario 2 are used,
``NEP_SCENARIO_2045`` in
:py:mod:`nep2025 <egon.data.datasets.hydrogen_etrago.nep2025>`). End points
more than 5 km from a node of the core network get their own ``H2_grid`` bus.
Converted sections have the costs of a retrofitted pipeline. The Kernnetz
measures that are not part of the network 2045 of scenario 2 (Tabelle 37:
Achim-Heidenau-Elbe Süd, replaced by the conversion Achim-Elbe Süd DN 1400,
Huntorf-Elsfleth 2, Böhlen-Borna, Borna-Thierbach) are removed, and the
Delta-Rhine-Corridor has DN 1000 as in scenario 2.

**Other H2 buses.** ``H2`` buses are created at the CH4 buses that are more
than 10 km from every ``H2_grid`` bus; they couple these CH4 nodes to the
hydrogen sector (methanation, SMR, local electrolysis). ``H2_saltcavern``
buses represent the salt cavern storage potential and are connected to the
nearest ``H2_grid`` bus (see the gas stores section).

Grids abroad and cross-border capacities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Abroad, the target scenarios have one CH4 bus per neighbouring country (AT,
BE, CH, CZ, DK, FR, GB, LU, NL, NO, PL, SE; RU is optional). The cross-border
capacities of the methane grid come from the TYNDP 2024 of ENTSOG, Annex C1
"Natural Gas Infrastructure Capacities" [TYNDP2024_gas]_ (cross-border points,
level "ADVANCED", interpolated between 2030, 2040 and 2050 to the year of the
scenario). The capacity of a border is distributed evenly over the pipelines
crossing it.

The hydrogen grid is connected abroad at the cross-border points (GÜP) of the
NEP [NEP_GasH2_2025]_, each by one link from the ``H2_grid`` bus at the
border to the hydrogen bus of the country (PyPSA-Eur), with the entry
(import) and exit (export) capacities of scenario 2: 2037 entry 62.4 GWh/h
(Tabelle 22, including the additional entry for the "Dunkelflaute" balance),
exit 6.6 GWh/h (transit test, Tabelle 31); 2045 entry 138.5 GWh/h and exit
30.3 GWh/h (Tabelle 36). The 2037 values are also used for eGon2035. All
capacities of the NEP are Brennwert; they are divided by 1.18
[SR_GasH2_2025]_ to the lower heating value of the model (2037 entry 52.9 GW
and exit 5.6 GW, 2045 entry 117.4 GW and exit 25.7 GW). AquaDuctus ("Norway/UK"
in the NEP) is split equally between NO and GB, Dornum/Emden is assigned to NO.

The hydrogen buses abroad have no supply of their own, so the imports are
modeled as ``H2`` generators: at the bus of each country with the entry
capacity of its border points, and, in 2045, at the H2_grid buses of the
converted LNG terminals Wilhelmshaven, Stade and Brunsbüttel (8.3, 7.0 and
4.4 GWh/h, Tabelle 36). The other imports of the NEP (LH2 and derivatives,
mostly ammonia: 4 GWh/h in 2037, 21 GWh/h in 2045, Tabellen 21 and 35) are
shared over the federal states like the import projects of the market survey
of the Szenariorahmen [SR_GasH2_2025_Annexes]_ (projects with at least the
status "Entwurfsplanung" in 2037, all projects in 2045) and placed at the
nearest ``H2_grid`` bus of their site: the LNG terminals in the coastal states,
as the approval of the Szenariorahmen asks [SR_GasH2_2025]_ (Wilhelmshaven and
Stade, Brunsbüttel, Rostock and Lubmin), elsewhere the largest chemical site
(Marl, Leuna, Schwedt, Böhlen, Burghausen; assumption). In total, the imports
are 56.3 GW in 2037 (52.9 GW abroad, 3.4 GW other imports) and 151.9 GW in
2045 (117.4 GW abroad, 16.7 GW LNG terminals, 17.8 GW other imports) on the
lower heating value. The capacity is fixed to the entry capacity of the NEP;
the price is the hydrogen price of the NEP Strom, which is derived from the
natural gas price and the CO2 cost (47.6 EUR/MWh in 2037, 51.0 EUR/MWh in 2045,
[NEP2025]_ Tabelle 9; same formula for eGon2035). There is no limit over the year.

The implementation abroad is detailed in :py:mod:`gas_neighbours.gas_scenarios
<egon.data.datasets.gas_neighbours.gas_scenarios>`.

In the gas sector, only the transport grids are represented: the methane
(CH4) transmission grid and the hydrogen (H2) core network. Both are modeled
as PyPSA *links* between *buses* of the carriers ``CH4``, ``H2_grid`` and
``H2``, with a fixed capacity and without losses. The target scenarios follow
the official network planning: the Netzentwicklungsplan Gas und Wasserstoff
2025 [NEP_GasH2_2025]_ has priority, and it refines the approved hydrogen
core network (Wasserstoff-Kernnetz) of the FNB Gas [Kernnetz]_.

Scenarios
~~~~~~~~~

The status quo scenario represents the observed system with a reduced depth.
The target scenarios use the same methods with the planning data of their
year:

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
   * - reGon2037
     - SciGRID_gas
     - Kernnetz and NEP measures commissioned until 2037
     - one CH4 bus per country (TYNDP 2024), H2 buses at the border points
       of the NEP
   * - reGon2045
     - none
     - Kernnetz and additional sections 2045 of the NEP
     - one CH4 bus per country (TYNDP 2024) without pipelines to Germany,
       H2 buses at the border points of the NEP

The implementation is detailed in :py:mod:`gas_grid <egon.data.datasets.gas_grid>`,
:py:mod:`hydrogen_etrago.bus <egon.data.datasets.hydrogen_etrago.bus>`,
:py:mod:`hydrogen_etrago.h2_grid <egon.data.datasets.hydrogen_etrago.h2_grid>`,
:py:mod:`hydrogen_etrago.nep2025 <egon.data.datasets.hydrogen_etrago.nep2025>`
and :py:mod:`gas_neighbours <egon.data.datasets.gas_neighbours>`.

**Planning data.** The published Excel files (Kernnetz lists of the FNB Gas,
annexes of the NEP Gas und Wasserstoff and of its Szenariorahmen,
consultation list of the Bundesnetzagentur, TYNDP 2024) are read unchanged
from the data bundle (folders ``hydrogen_network``, ``NEP/NEP_gas_H2`` and
``gas_data``) and downloaded from their source if they are missing there. The
tables that the NEP only publishes in its PDF are read from
``NEP/NEP_gas_H2/NEP_Gas_H2_2025_tables.xlsx`` of the data bundle (one sheet
per table, with its source). Where the NEP Gas und Wasserstoff and the NEP
Strom differ, the NEP Strom (scenario C) is followed, as the electricity
sector of the target scenarios is based on it.

**Energy basis.** The NEP Gas und Wasserstoff gives capacities on the higher
heating value (Brennwert). The hydrogen side of the model uses the lower
heating value, so the hydrogen values of the NEP are divided by 1.18
[SR_GasH2_2025]_. The methane side keeps the basis of its sources: the
infrastructure is on the higher heating value, the demand on the lower one.

Methane grid
~~~~~~~~~~~~

The methane grid is based on the SciGRID_gas data model [SciGRID_gas]_
(IGGIELGN data set). The nodes of the German grid are CH4 buses, the
pipelines are bidirectional links. Their capacity is derived from the
diameter of the pipelines with the classification of [Kunz]_. The length of
the links is given in km.

**Status quo (status2024).** Germany is one CH4 bus without pipelines. It is
supplied by a slack generator bounded by the domestic production over the
year (see the gas supply section).

**Pipelines converted to hydrogen (reGon2037).** The methane grid keeps the
pipelines that the Kernnetz lists of the FNB Gas and the NEP convert to
hydrogen. The methane distribution network of 2037 is not known, and
removing the converted pipelines from the methane grid would cut off CH4
buses with their demand and supply. The converted pipelines are part of the
hydrogen grid as well, as ``H2_grid`` links parallel to the methane
pipelines.

**No methane in Germany (reGon2045).** Germany has no methane system: no
CH4 buses, pipelines, production, stores or industrial demand. This is a model assumption for scenario 2; the methane network 2045
of the NEP Gas und Wasserstoff 2025 (Anhang 5) is modeled for scenario 3.
Biomethane is not modeled in 2045. Abroad, the CH4 buses and the pipelines
between them remain.

**Not modeled.** The methane expansion measures of the NEP up to 2037 are
mostly pressure regulation and metering stations, reconnections and system
separations, below the resolution of SciGRID_gas.

Hydrogen grid
~~~~~~~~~~~~~

**Hydrogen core network (reGon2037, reGon2045).** The pipelines come from
the Kernnetz lists of the FNB Gas of 10 December 2024 [Kernnetz]_ (new
builds, conversions of CH4 pipelines and pipelines of further operators,
e.g. Gasnetz Hamburg). The approved pipelines are always part of the network
and are not extendable. The NEP drops three Kernnetz measures (Anhang 2) but replaces
each of them by another measure on nearly the same route (e.g. H2-201
Überackern-Haiming for KLN001-01), so they are kept as proxies of their
replacements. The commissioning year of the NEP (Anlage 1b; the latest date
of the NEP measures of a Kernnetz measure, assigned by Tabelle 11,
Tabelle 13 and Anhang 9) replaces the one of the list where the NEP gives
it. A pipeline is only part of a scenario if it is commissioned by the year
of the scenario. The costs come from the investment of the list or, if not
given, from the specific costs of a new H2 pipeline.

**Pipeline capacity.** The capacity of a pipeline follows from its nominal
diameter (DN) and design pressure (DP). It is scaled from the design
capacities of the hydraulic simulations of the European gas TSOs for the
European Hydrogen Backbone [EHB2021]_ (13 GW for DN 1200 at 80 bar, 1.2 GW
for DN 500 at 50 bar): linear in the pressure and with a power of the
diameter fitted to these two points. Where the lists give no DN or DP, DN
400 and 70 bar are assumed.

**Nodes.** The end points of the pipelines are named places, located with
the node list ``hydrogen_network/h2_grid_nodes.csv`` of the data bundle. The
list is extended by the places of the Kernnetz lists and of the NEP sections
(see below) that it lacked: the coordinates of the municipality or of the
site (OpenStreetMap); a different spelling of a listed place (e.g. "Legden"
for "Ledgen") or a place whose Kernnetz node is listed ("Gernsheim" for
"Gernsheim-Süd") gets the coordinates of that node. The list also locates the
sites of the hydrogen imports and of the LNG terminals (see the gas supply
section). Only nodes with a
pipeline in the scenario year become ``H2_grid`` buses, and places with two
names but the same coordinates (e.g. "Hittistetten" and "Hittistetten
(Senden)") are one bus. Some pipelines are split at intermediate nodes following the detailed
map of the FNB Gas; length and investment of a split pipeline are shared
between its parts in proportion to their straight distance.

**Isolated parts.** Some parts of the network are not connected to the rest
by any pipeline of the lists, because the lists name the places coarsely
(e.g. Hamburg Süd/Mitte/Ost, Moosburg, Amelsbüren/Rinkerode/Uentrop,
Perl/Besch). They are approved pipelines as well, so each of them is
connected by a straight pipeline from its bus closest to the rest of the
network to the nearest bus there, with the capacity of its largest pipeline
and the commissioning year of its first one (assumption). Parts that a
neighbouring country can supply (e.g. Freiburg via Fessenheim) keep that
connection only; a cross-border point for exports only does not count. The
same applies to measures of the NEP whose connection to the network the NEP
does not name (e.g. Dernbach-Bendorf, Weißenhorn).

**Known limitation.** Pipelines whose start and end are the same named place
can not be placed and are not inserted, mostly the pipelines of Gasnetz
Hamburg within "Hamburg Süd" and "Hamburg Mitte". The Hamburg network is
represented by the pipelines Hamburg Süd - Mitte - Ost and its connection to
the rest of the network.

**Hydrogen measures of the NEP up to 2037.** The NEP adds measures beyond
the Kernnetz (Anlage 3 of [NEP_GasH2_2025]_). Used are the ones of the
modeling result 2037 of scenario 2, on which the network 2045 of the NEP
builds: the network proposal (criteria H2(4) and H2(6)) and the measures of
scenario 2 that are not part of the proposal (criterion H2(5)). A measure is
part of a scenario from its commissioning year: the date of the consultation
list of the Bundesnetzagentur [BNetzA_IBN2026]_ where given, otherwise
12/2036 as stated by the NEP. The NEP gives neither length nor DN/DP of these
measures, so they are assumed:

* *Length*: Tabelle 29 of the NEP where given; for H2-233 (the former
  Kernnetz measure KLU121-01) the FNB Gas application list of 22 July 2024;
  otherwise the straight line times the median ratio of the length to the
  straight line of the sections of Anhang 6.
* *DN/DP*: the SciGRID_gas pipelines along the route for long measures,
  otherwise the median of the conversions of Anhang 6 (DN 600 / 70 bar from
  15 km, DN 400 / 70 bar below).

Measures within one place and "Isarschiene Ost/West" (no end points
published) are not placed.

**Placing the NEP sections.** A section of the NEP becomes a link between
the ``H2_grid`` buses nearest to its end points; an end point more than 5 km
from every node of the network gets its own ``H2_grid`` bus. A section whose
end points fall on the same bus lies within one node and is not inserted,
nor is a section whose buses are more than twice its length (plus 5 km)
apart. Like the Kernnetz pipelines, the sections are given (not extendable);
as the NEP publishes no investment, they have the specific costs of a new H2
pipeline.

**Hydrogen network 2045 (reGon2045).** In addition, the sections of the NEP
beyond the modeling result 2037 are inserted (Anhang 6, the sections of
scenario 2, ``NEP_SCENARIO_2045`` in
:py:mod:`nep2025 <egon.data.datasets.hydrogen_etrago.nep2025>`). The Kernnetz measures
that are not part of the network 2045 of scenario 2 are removed, and the
Delta-Rhine-Corridor gets the diameter of scenario 2 (Tabelle 37).

**Other H2 buses.** ``H2`` buses are created at the other nodes of the node
list (without a pipeline in the scenario year) that are more than 10 km from
every ``H2_grid`` bus; they carry the industrial hydrogen demand far from the
network (see the gas demand section) and local electrolysis. In reGon2037,
``H2`` buses are also created at the CH4 buses that are more than 10 km from
every other hydrogen bus, to couple these CH4 nodes to the hydrogen sector
(methanation, SMR, local electrolysis); they have no Voronoi area and get no
industrial hydrogen demand. ``H2_saltcavern``
buses represent the salt cavern storage potential and are connected to the
nearest ``H2_grid`` bus (see the gas stores section).

Grids abroad and cross-border capacities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Abroad, the target scenarios have one CH4 bus per neighbouring country (AT,
BE, CH, CZ, DK, FR, GB, LU, NL, NO, PL, SE; RU is optional). The
cross-border capacities of the methane grid come from the TYNDP 2024 of
ENTSOG, Annex C1 "Natural Gas Infrastructure Capacities" [TYNDP2024_gas]_
(cross-border points, level "ADVANCED", interpolated between 2030, 2040 and
2050 to the year of the scenario). The capacity of a border is distributed
evenly over the pipelines crossing it; in reGon2045 no pipeline crosses the
German border.

The hydrogen grid is connected abroad at the cross-border points (GÜP) of
the NEP [NEP_GasH2_2025]_, each by one link from the ``H2_grid`` bus at the
border to the hydrogen bus of the country (PyPSA-Eur). The link has the
larger of the entry (import) and exit (export) capacity of scenario 2 and is
limited to each of them by direction: in 2037 the entry of Tabelle 22
(including the additional entry for the "Dunkelflaute" balance) and the exit
of the transit test (Tabelle 31), in 2045 the entry and exit of Tabelle 36.
A border point of two countries is split: AquaDuctus ("Norway/UK" in the
NEP) equally between NO and GB; Dornum/Emden is assigned to NO. The hydrogen
buses of countries without a border point are removed. The imports at the
border points are described in the gas supply section.

The implementation abroad is detailed in :py:mod:`gas_neighbours.gas_scenarios
<egon.data.datasets.gas_neighbours.gas_scenarios>` and
:py:mod:`hydrogen_etrago.h2_grid <egon.data.datasets.hydrogen_etrago.h2_grid>`.

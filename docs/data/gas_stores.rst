Methane stores
~~~~~~~~~~~~~~

Methane is stored in underground storage sites and in the grid (line pack).
Both are PyPSA *stores* with an invariable capacity. The status quo scenario
(one CH4 bus for Germany) has no methane stores.

**Underground storage in Germany.** The sites of SciGRID_gas [SciGRID_gas]_
are classified as cavern or pore storage by the site list of the INES study
[INES2022]_ (Anhang 1). Bad Lauchstädt, which has both, counts as a cavern
site, as its caverns are the ones converted to hydrogen; the anonymous
entries of SciGRID_gas are not classified.

The capacity of the scenario (``CH4_storage_capacity``) follows the role of
the stores in the peak load case of the NEP Gas und Wasserstoff 2025
[NEP_GasH2_2025]_ (Tabelle 19): the working gas volume of the German stores
(GIE, 2021) is reduced in proportion to their feed-in into the grid in the
scenario year relative to 2030 (scenario 4). In reGon2037 this is the feed-in
of scenario 2; reGon2045 has no methane stores, as the NEP only has a feed-in
from methane stores in 2045 in scenario 3.

The capacity is taken from the pore storage and the sites the INES list does
not classify first and from the caverns last, as the caverns are the
candidates for the conversion to hydrogen storage.

**Line pack.** The storage capacity of the German methane grid
(``CH4_grid_capacity``) is the line pack of the whole grid [Bna]_. It is
distributed over the CH4 buses in proportion to the volume of the pipelines
at each bus. reGon2045 has no methane grid in Germany and so no line pack.

**Abroad**, the methane storage capacities per country come from SciGRID_gas.

The implementation is detailed in :py:mod:`ch4_storages <egon.data.datasets.ch4_storages>`.

Hydrogen stores
~~~~~~~~~~~~~~~

Hydrogen is stored in steel tanks (``H2_overground``, at every ``H2_grid`` and
``H2`` bus, not limited) and in salt caverns (``H2_underground``). Both are
extendable PyPSA *stores* with costs from the technology data [technoData]_.
The status quo scenario has no hydrogen stores.

The salt cavern potential lies at the intersection of the substations and the
salt structures suitable for caverns of the InSpEE-DS study [BGR]_; it is the
upper limit of the store (``e_nom_max``) at an ``H2_saltcavern`` bus, which is
connected to the nearest ``H2_grid`` bus. The potential is not reduced by the
methane caverns converted to hydrogen.

**Converted methane caverns.** The cavern volume that leaves the methane
system (see above) is stored as hydrogen in a not extendable
``H2_underground`` store at the ``H2_saltcavern`` bus of its area: the
methane working gas of the site times the ratio of the working gas energy of
the German caverns with hydrogen to that with natural gas, both on the lower
heating value ([INES2022]_, Tabelle 44). It shares the link to the grid with
the potential of new caverns at that bus (see below).

The link to the grid is extendable up to the storage capacity of scenario 2 of
the NEP Gas und Wasserstoff 2025 [NEP_GasH2_2025]_ (Tabellen 20, 21, 34 and
35): the withdrawal, including the additional withdrawal of the load case
"Dunkelflaute", on the lower heating value. The withdrawal is shared over the
federal states like the storage projects of the market survey of the
Szenariorahmen [SR_GasH2_2025_Annexes]_ (projects with at least the status
"Entwurfsplanung" in 2037, all projects in 2045); the additional withdrawal
of 2037, which the NEP assigns to four storage sites, goes equally to their
four states. The shares of the states without salt structures (e.g. Bavaria,
Hesse) go to the others. Within a state it is
shared by the hydrogen volume at each ``H2_saltcavern`` bus: the potential of
new caverns plus the converted methane caverns. The injection is limited
to its ratio to the withdrawal in the NEP (``p_max_pu``). The NEP gives no
working gas volume, so beyond the converted caverns the volume of the stores
is left to the optimisation within the potential.

The implementation is detailed in :py:mod:`hydrogen_etrago.storage
<egon.data.datasets.hydrogen_etrago.storage>`.

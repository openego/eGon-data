Methane stores
~~~~~~~~~~~~~~

Methane is stored in underground storage sites and in the grid (line pack).
Both are PyPSA *stores* with an invariable capacity. The status quo scenario
(one CH4 bus for Germany) has no methane stores.

**Underground storage in Germany.** The sites of SciGRID_gas [SciGRID_gas]_
are classified as cavern or pore storage by the site list of the INES study
[INES2022]_ (Anhang 1). Bad Lauchstädt, which has both, counts as a cavern
site, as its caverns are the ones converted to hydrogen; the anonymous
entries of SciGRID_gas are not classified. The capacity of the scenario (``CH4_storage_capacity``) follows
the feed-in of the stores into the grid in the peak load case of the NEP Gas
und Wasserstoff 2025 [NEP_GasH2_2025]_ (Tabelle 19; the minimum of the
approved Szenariorahmen is 130 GWh/h in 2030 and 37 GWh/h in 2037):

.. list-table:: Methane storage capacity in Germany
   :widths: 18 18 64
   :header-rows: 1

   * - Scenario
     - Capacity [TWh]
     - Basis
   * - eGon2035
     - 171.3
     - feed-in 186 GWh/h in 2030 (scenario 4) to 93 GWh/h in 2037
       (scenario 2), linear to 2035, applied to the working gas volume of today
   * - reGon2037
     - 133.3
     - 93 of 186 GWh/h feed-in in 2037 (scenario 2)
   * - reGon2045
     - 0
     - no feed-in from methane stores in 2045 (only in scenario 3 of the NEP)

The capacity is taken from the pore storage and the sites the INES list does
not classify first and from the caverns last, as the caverns are the
candidates for the conversion to hydrogen storage (eGon2035: 40.3 of
135.4 TWh cavern volume kept, reGon2037: 2.2 TWh).

**Line pack.** The storage capacity of the German methane grid
(``CH4_grid_capacity``: 114,200 MWh in eGon2035 and reGon2037, the grid of
130 GWh [Bna]_ without the pipelines converted to hydrogen; 17,900 MWh in
reGon2045, the share of the methane network 2045 of the NEP) is distributed
over the CH4 buses in proportion to the volume of the pipelines at each bus.
In reGon2045 only the pipelines of the methane network 2045 count.

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

The link to the grid is extendable up to the storage capacity of scenario 2 of
the NEP Gas und Wasserstoff 2025 [NEP_GasH2_2025]_ (Tabellen 20, 21, 34, 35):
withdrawal 36 GWh/h plus 2.4 GWh/h in the load case "Dunkelflaute" and
injection 26 GWh/h in 2037 (also used for eGon2035), withdrawal 53 GWh/h and
injection 38 GWh/h in 2045. The NEP values are Brennwert and are divided by
1.18 [SR_GasH2_2025]_ (32.5 and 44.9 GW on the lower heating value). The
withdrawal is shared over the federal states like the storage projects of the
market survey of the Szenariorahmen [SR_GasH2_2025_Annexes]_ (projects with at
least the status "Entwurfsplanung" in 2037, all projects in 2045; the
"Dunkelflaute" part equally over its four states); the shares of the states
without salt structures (e.g. Bavaria, Hesse) go to the others. Within a state
it is shared by the storage potential of the salt caverns. The injection is
limited to its share of the withdrawal (``p_max_pu`` 0.68 in 2037, 0.72 in
2045). The NEP gives no working gas volume.

The implementation is detailed in :py:mod:`hydrogen_etrago.storage
<egon.data.datasets.hydrogen_etrago.storage>`.

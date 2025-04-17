In the gas sector, only the transport grids are represented. They are modelled
with bidirectional PyPSA *links* which allow to represent the energy exchange
between distinct PyPSA *buses*. No losses are considered and the
energy that can be transferred through a pipeline is limited by the e_nom
parameter.

In the scenario **eGon2035**, only the methane grid is modelled.

In the scenario **eGon100RE**, there are tree type of gas grid:

* the remaining methane grid,
* the retrofitted hydrogen grid,
* the potential hydrogen grid extension (only present in Germany).

In Germany
~~~~~~~~~~

To determine the topology of the grid, the status quo methane grid is used,
with the data from SciGRID_gas [SciGRID_gas]_.

eGon2035
""""""""

In the scenario eGon2035, the capacities of the methane pipelines are determined
using the status quo pipeline diameters [SciGRID_gas]_ and the correspondence
table [Kunz]_. These capacities are fixed.

There are two types of hydrogen buses and none of them are connected through
a grid:

* H2_grid buses: these buses are located at the places than the methane
  buses,
* H2_saltcavern buses: these buses are located at the intersection of power
  substations and potential saltcavern areas, adapted for hydrogen storage.

In this scenario, there are no hydrogen bus modelled in the neighboring
countries.

The modelling of the methane grid in the scenario eGon2035 is represented
below.

.. image:: images/2035_gasgrid_enTitle_00.png
   :width: 400

The implementation of these data is detailed respectively in the
:py:mod:`gas_grid <egon.data.datasets.gas_grid>` and :py:mod:`hydrogen_etrago.bus
<egon.data.datasets.hydrogen_etrago.bus>` pages of our documentation.

eGon100RE
"""""""""

In the scenario eGon100RE, the methane grid has the same topology than in
the scenario eGon2035. The capacities of the methane pipelines are still
fixed, but they are reduced, because a part of the grid is retrofitted
into an hydrogen grid.

The retrofitted hydrogen grid has the same topology than the methane grid.
The share of the methane grid which is retrofitted is calculated by the
PyPSA-eur-sec run.

The potential hydrogen grid extensions link each hydrogen saltcavern to
its nearest (retrofitted) hydrogen grid bus. The extension has no upper
boundary and its technical parameters are taken from the PyPSA technology
data [technoData]_. This option is modelled exclusively in Germany.

The modelling of the methane grid in the scenario eGon100RE is represented
below.

.. image:: images/100RE_gasgrid_enTitle_00.png
   :width: 400

The implementation of these data is detailed respectively in the :py:mod:`gas_grid
<egon.data.datasets.gas_grid>` and :py:mod:`h2_grid <egon.data.datasets.hydrogen_etrago.h2_grid>`
pages of our documentation.


Cross-bordering pipelines
~~~~~~~~~~~~~~~~~~~~~~~~~

The cross-bordering pipelines capacities are taken respectively from the
TYNDP [TYNDP]_ for eGon2035 and from PyPSA-eur-sec for eGon100RE.

For the cross-bordering gas pipelines with Germany, the capacity is uniformly
distributed between the all pipelines connecting one country to Germany.

The implementation of these data is detailed respectively in the
:py:func:`gas_neighbours.eGon2035.grid <egon.data.datasets.gas_neighbours.eGon2035.grid>` and
:py:func:`insert_gas_neigbours_eGon100RE <egon.data.datasets.gas_neighbours.eGon100RE.insert_gas_neigbours_eGon100RE>`
pages of our documentation.

Hydrogen stores
~~~~~~~~~~~~~~~

In both scenarios is there the option to store hydrogen. In the scenario
eGon2035, these potentials are modelled only in Germany, because the hydrogen
sector is not modelled in the neighboring countries.
Hydrogen is stored either in steel tanks (*overground storage*) or in saltcavern
(*underground storage*), which are modelled by PyPSA *stores*. The technical 
parameters (investment and marginal costs, efficiency, lifetime) comes
from the PyPSA technology data [technoData]_. The steel tank storage potentials
are not limited. On the contrary, the saltcavern storage potentials do have
an upper boundary.

In Germany
""""""""""

The saltcavern potentials are located at the intersection between the power
substations and the adapted saltcavern surface areas, defined in the InSpEE-DS
report from the Bundesanstalt für Geowissenschaften und Rohstoffe [BGR]_.
These potentials, identical in the both scenarios, are represented in the
figure below.

.. image:: images/H2_underground_stores_withTitle.png
   :width: 400

The implementation of these data is detailed in the :py:mod:`hydrogen_etrago.storage
<egon.data.datasets.hydrogen_etrago.storage>` page of our documentation.

In eGon100RE, the retrofitted hydrogen grid can also be used as storage,
with unvariable capacity. This implementation is detailed in the 
:py:mod:`grid_storage <egon.data.datasets.ch4_storages>` page of our documentation.

In the neighboring countries
""""""""""""""""""""""""""""

In the scenario eGon100RE the hydrogen storage potentials in the neighboring
countries are taken directly from the PyPSA-eur-sec run.

Methane stores
~~~~~~~~~~~~~~

The methane stores are modelled as PyPSA *stores* and they are invariable.
In Germany, two types of methane stores are modelled:

* the underground caverns: capacities from SciGRID_gas [SciGRID_gas]_,
* the storage capacity of the German gas grid.

The storage capacity of the gas grid (130 GWh according to the Bundesnetzagentur
[Bna]_) is uniformly distributed between all the methane nodes in Germany
in the scenario eGon2035.
In the scenario eGon100RE, the retrofitted hydrogen grid take a share of
this storage capacity over.

The implementation of the methane storage in the data model is detailed in the
:py:mod:`ch4_storages <egon.data.datasets.ch4_storages>` page of our documentation.

In the neighboring countries, the methane store capacities from SciGRID_gas [SciGRID_gas]_
are used in the eGon2035 scenario and data from PyPSA-eur-sec are used in
the eGon100RE scenario.

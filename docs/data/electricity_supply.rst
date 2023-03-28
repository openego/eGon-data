Gas turbines
~~~~~~~~~~~~

The gas turbines, or open cycle gas turbines (OCGTs) allow the production
of electricity from methane and are modelled with unidirectional PyPSA *links*,
which connect methane buses to power buses.

The capacities of the gas turbines are invariable and considered as constant.
The technical parameters (investment and marginal costs, efficiency, lifetime)
comes from the PyPSA technology data [technoData]_.

In Germany
""""""""""

In Germany, the gas turbines listed in the Netzentwicklungsplan [NEP2021]_
are matched to the Marktstammdatenregister in order to get their geographical
coordinates in :py:func:`allocate_conventional_non_chp_power_plants
<egon.data.datasets.power_plants.allocate_conventional_non_chp_power_plants>`.
The matched units are then associated to the corresponding power and methane
buses.

The implementation of gas turbines in the data model is detailed in the
:py:mod:`OpenCycleGasTurbineEtrago <egon.data.datasets.power_etrago.OpenCycleGasTurbineEtrago>`
page of our documentation.

**Warning**

OCGT in Germany are still missing in eGon100RE: https://github.com/openego/eGon-data/issues/983

In the neighboring countries
""""""""""""""""""""""""""""

In the scenario eGon2035, the gas turbines capacities abroad comes from the
TYNDP 2035 [TYNDP]_, the implementation is detailed in :py:func:`eGon2035.tyndp_gas_generation
<egon.data.datasets.gas_neighbours.eGon2035.tyndp_gas_generation>`.

In the scenario eGon100RE the gas turbines capacities in the neighboring
countries are taken directly from the PyPSA-eur-sec run.

Fuel cells
~~~~~~~~~~

The fuel cells allow the production of electricity from hydrogen and are
modelled with unidirectional PyPSA *links*, which connect hydrogen buses
to power buses.

The data model contains the potentials of this technology, whose capacities
are optimized. The technical parameters (investment and marginal costs,
efficiency, lifetime) comes from the PyPSA technology data [technoData]_.

In the eGon2035 scenario, fuel cells are modelled at every hydrogen bus
in Germany, as well as in the eGon100RE scenario. In eGon100RE, this technology
is also modelled in the neighboring countries. In Germany, the potentials
are generally not limited, except when the connected buses are located more
than 500m far from each other. In this particular case, the potential has
an upper limit of 1 MW.

The implementation of fuel cells in the data model is detailed in the
:py:mod:`power_to_h2 <egon.data.datasets.hydrogen_etrago.power_to_h2>`
page of our documentation.



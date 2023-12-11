The electrical power plants park, including data on geolocations, installed capacities, etc.
for the different scenarios is set up in the dataset
:class:`PowerPlants<egon.data.datasets.power_plants.PowerPlants>`.

Main inputs into the dataset are target capacities per technology and federal state
in each scenario (see :ref:`concept-and-scenarios-ref`) as well as the MaStR (see :ref:`mastr-ref`),
OpenStreetMap (see :ref:`osm-ref`) and potential areas (provided through the data bundle,
see :ref:`data-bundle-ref`) to distribute the generator capacities within each federal state region.
The approach taken to distribute the target capacities within each federal state differs for
the different technologies and is described in the following.
The final distribution in the eGon2035 scenario is shown in figure :ref:`generator-park-egon-2035`.

.. figure:: /images/Erzeugerpark.png
  :name: generator-park-egon-2035
  :width: 400

  Generator park in the eGon2035 scenario

Onshore wind
+++++++++++++

Offshore wind
++++++++++++++

PV ground mounted
++++++++++++++++++

PV rooftop
+++++++++++

In a first step, the target capacity in the eGon2035 and eGon100RE scenarios is distributed
to all MV grid districts linear to the residential and CTS electricity demands in the
grid district (done in function
:func:`pv_rooftop_per_mv_grid<egon.data.datasets.power_plants.pv_rooftop.pv_rooftop_per_mv_grid>`).

Afterwards, the PV rooftop capacity per MV grid district is disaggregated
to individual buildings inside the grid district (done in function
:func:`pv_rooftop_to_buildings<egon.data.datasets.power_plants.pv_rooftop_buildings.pv_rooftop_to_buildings>`).
The basis for this is data from the MaStR, which is first cleaned and missing information
inferred, and then allocated to specific buildings. New PV plants are in a last step
added based on the capacity distribution from MaStR.
These steps are in more detail described in the following.

MaStR data cleaning and inference:

* Drop duplicates and entries with missing critical data.
* Determine most plausible capacity from multiple values given in MaStR data.
* Drop generators that don't have a plausible capacity (23.5 MW > P > 0.1 kW).
* Randomly and weighted add a start-up date if it is missing.
* Extract zip and municipality from 'site' given in MaStR data.
* Geocode unique zip and municipality combinations with Nominatim (1 sec
  delay). Drop generators for which geocoding failed or which are located
  outside the municipalities of Germany.
* Add some visual sanity checks for cleaned data.

Allocation of MaStR plants to buildings:

* Allocate each generator to an existing building from OSM or a synthetic building
  (see :ref:`building-data-ref`).
* Determine the quantile each generator and building is in depending on the
  capacity of the generator and the area of the polygon of the building.
* Randomly distribute generators within each municipality preferably within
  the same building area quantile as the generators are capacity wise.
* If not enough buildings exist within a municipality and quantile additional
  buildings from other quantiles are chosen randomly.

Disaggregation of PV rooftop scenario capacities:

* The scenario data per federal state is linearly distributed to the MV grid
  districts according to the PV rooftop potential per MV grid district.
* The rooftop potential is estimated from the building area given from the OSM
  buildings.
* Grid districts, which are located in several federal states, are allocated
  PV capacity according to their respective roof potential in the individual
  federal states.
* The disaggregation of PV plants within a grid district respects existing
  plants from MaStR, which did not reach their end of life.
* New PV plants are randomly and weighted generated using the capacity distribution of
  PV rooftop plants from MaStR.
* Plant metadata (e.g. plant orientation) is also added randomly and weighted
  using MaStR data as basis.

Hydro
++++++

Gas turbines
++++++++++++

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
++++++++++

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
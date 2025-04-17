Direct methane production
~~~~~~~~~~~~~~~~~~~~~~~~~

Methane is directly produced, what is modelled through PyPSA *generators*. These
generators have invariable capacities.

In the scenario eGon2035 the production of biogas as well as the production
of natural gas are represented. In the scenario eGon100RE, only biogas is
produced. These production potentials are considered as constant.

**In Germany**, the extraction potentials of natural gas are taken from
the SciGRID_gas data [SciGRID_gas]_. They are only used in the eGon2035 scenario.
The biogas production potentials are from the Biogaspartner Einspeiseatlas
[Einspeiseatlas]_ and are in both scenarios. The spatial repartition
of the methane production potentials in Germany is represented
in the figure below: left, the biogas production potentials (in green) and
right the natural gas production potential (in grey).

.. image:: images/GasGen_eGon2035.png
   :width: 800

In the scenario eGon2035, the German production over the year of natural
gas and of biogas is limited by values from the Netzentwicklungsplan Gas
2020–2030 [NEP_gas]_ (36 TWh natural gas and 10 TWh biogas). In the
scenario eGon100RE, the German biogas production is limited by a value
calculated in the PyPSA-eur-sec run (14.45 TWh).

The implementation of these data is detailed in the :py:mod:`ch4_prod
<egon.data.datasets.ch4_prod>` page of our documentation.

**In the neighboring countries**, the methane production potentials includes
biogas and natural gas potential (with data from the TYNDP), as well as
LNG terminals import potential (with data from SciGRID_gas). The implementation
of these data is detailed in the :py:mod:`gas_neighbours.eGon2035
<egon.data.datasets.gas_neighbours.eGon2035>` page of our documentation.

In the scenario eGon100RE, the biogas production potentials of the neighboring
countries are taken directly from the PyPSA-eur-sec run.

The following table summarize the multiple sources used to model the methane
production potentials in the both scenarios.

.. table:: References overview of the methane production potentials

  +--------+-----------------------------+--------------------+-----------------+--------------------+
  |                                      | eGon2035                             |  eGon100RE         |
  +                                      +--------------------+-----------------+--------------------+
  |                                      |  Natural gas       |  Biogas         |  Biogas            |
  +========+=============================+====================+=================+====================+
  |Germany |Regionalization              | SciGRID_gas        |  Biogaspartner  |  Biogaspartner     |
  |        |                             |                    |                 |                    |
  |        |                             |                    |  Einspeiseatlas |  Einspeiseatlas    |
  +        +-----------------------------+--------------------+-----------------+--------------------+
  |        |Max production over the year | NEP Gas            | NEP Gas         | PyPSA-eur-sec run  |
  +--------+-----------------------------+--------------------+-----------------+--------------------+
  | Neighbouring countries               | TYNDP                                | PyPSA-eur-sec run  |
  |                                      |                                      |                    |
  |                                      | *Also includes LNG terminal*         |                    |
  |                                      |                                      |                    |
  |                                      | *import potentials from SciGRID_gas* |                    |
  +--------+-----------------------------+--------------------+-----------------+--------------------+


Methanation and SMR
~~~~~~~~~~~~~~~~~~~

The methanation is the production of methane from hydrogen and the Steam
Methane Reforming (SMR) allows the reverse process: producing hydrogen with
methane.

Methanation and SMR are modelled with unidirectional PyPSA *links*, which
connect methane buses to hydrogen buses. The data model contains the potentials
of these technologies, whose capacities are optimized. The technical parameters
(investment and marginal costs, efficiency, lifetime) comes from the PyPSA
technology data [technoData]_.

In both scenarios, methanation and SMR are modelled at every methane bus
in Germany. Each one corresponding to a H2_grid bus, only these hydrogen
buses do have these technologies attached (in other words, H2_saltcavern
buses are not involved in methanation and SMR links). In both scenarios,
the potentials are not limited. In the eGon100RE scenario, these technologies
are also modelled in the neighboring countries.

The implementation of methanation and SMR in the data model is detailed
in the :py:mod:`h2_to_ch4 <egon.data.datasets.hydrogen_etrago.h2_to_ch4>`
page of our documentation.


Hydrogen feedin into the methane grid
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The hydrogen feedin is modelled **only in the scenario eGon2035** and corresponds
to the direct introduction of hydrogen into the methane grid. The hydrogen
feedin is modelled with unidirectional PyPSA *links*, which connect hydrogen
buses to methane buses.
This transformation is possible at every methane bus with a invariable
capacity calculated as 15% of the sum of the methane pipeline capacities
at this specific bus.

**Warning**

We found out that this very simplified model does not work good enough because
the constant capacity of the feedin links does not account for the flow
in the pipelines, which varies in the times.

The implementation of hydrogen feedin in the data model is detailed in
the :py:mod:`h2_to_ch4 <egon.data.datasets.hydrogen_etrago.h2_to_ch4>`
page of our documentation.


Electrolysis and fuel cells
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The electrolysis, the production of hydrogen from electricity is modelled
with unidirectional PyPSA *links*, which connect power buses to hydrogen buses.

The data model contains the potentials of this technology, whose capacities
are optimized. The technical parameters (investment and marginal costs,
efficiency, lifetime) comes from the PyPSA technology data [technoData]_.

In the eGon2035 scenario, electrolysis is modelled at every hydrogen bus
in Germany, as well as in the eGon100RE scenario. In eGon100RE, this technology
is also modelled in the neighboring countries. In Germany, the potentials
are generally not limited, except when the connected buses are located more
than 500m far from each other. In this particular case, the potential has
an upper limit of 1 MW.

The implementation of electrolysis in the data model is detailed in the
:py:mod:`power_to_h2 <egon.data.datasets.hydrogen_etrago.power_to_h2>`
page of our documentation.

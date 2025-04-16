The industrial gas demand is modelled with PyPSA *loads*.

In Germany
~~~~~~~~~~

In the scenario eGon2035, the industrial gas loads in Germany are taken
directly from the eXtremos project data [eXtremos]_. They model the hourly-resolved
industrial loads in Germany with a NUTS-3 spatial resolution, for methane
(:math:`CH_4`), as well as for hydrogen (:math:`H_2`), for the year 2035.

The spatial repartition of these loads is represented in the figure below
(methane demand in grey and hydrogen in cyan). The size of the rounds on
the figure corresponds to the total annual demand at the considered spot
in the scenario eGon2035.

.. image:: images/eGon2035_gas_ind_load_repartition_DE_withTitle.png
   :width: 400

In eGon100RE, the global industrial demand for methane and hydrogen (for
whole Germany and for one year) is calculated by the PyPSA-eur-sec run.
The spatial and the temporal repartitions used to distribute these values
are corresponding to the spatial and temporal repartition of the hydrogen
industrial demand from the eXtremos project [eXtremos]_ for the year 2050.
(The same repartition is used, due to the lack of data were available for
methane, because the eXtremos project considers that there won't be any
industrial methane load in 2050.)

.. image:: images/ind_gas_demand.png
   :width: 400

The figure above shows the temporal evolution of the methane (in grey) and
hydrogen (in cyan) industrial demands in the year for both scenarios
(eGon100RE in dashed).
The total demands for whole Germany are to be found in the following table.

.. list-table:: Total indutrial hydrogen and methane demands in Germany considered in eGon-data
   :widths: 25 25 25
   :header-rows: 1

   * -
     - eGon2035
     - eGon100RE
   * - :math:`CH_4` (in TWh)
     - 195
     - 105
   * - :math:`H_2` (in TWh)
     - 16
     - 42

The hydrogen loads are attributed only to the *H2_grid* buses.

The implementation of these data is detailed in the :py:mod:`industrial_gas_demand
<egon.data.datasets.industrial_gas_demand>` page of our documentation.

In the neighboring countries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the scenario **eGon2035**, there are no hydrogen buses modelled in the
neighboring countries. For this reason, the hydrogen industrial demand
is there attributed to the electrical sector.
The total demand is taken from the 'Distributed Energy' scenario of the
Ten-Year Network Development Plan 2020 ([TYNDP]_). For the year
2035, it has been linearly interpolated between the values for the year 2030
and 2040. These industrial hydrogen loads are considered as constant in
time. In other words, at each hour of the year, the same load should by
fulfilled by electricity to supply the hydrogen industrial demand.

Contrary to all the other loads described in the section, the modelled
methane load abroad includes not only the industrial demand, but also the
heat demand, that is supplied by methane. The total demand is again taken
from the 'Distributed Energy' scenario of the TYNDP 2020 (linear interpolation
between 2030 and 2040). For the temporal disaggregation of the demand in
the year, the time series 'rural heat' from PyPSA-eur-sec is used to approximated
the temporal profile, because the heat sector represents the biggest load.

The implementation of these data is detailed in the :py:mod:`gas_neighbours.eGon2035
<egon.data.datasets.gas_neighbours.eGon2035>` page of our documentation.

In the scenario **eGon100RE**, the industrial gas loads (for methane as
well as for hydrogen) in the neighboring countries are directly imported
from the PyPSA-eur-sec run.

Demand-side management (DSM) potentials are calculated in function :func:`dsm_cts_ind_processing<egon.data.datasets.DSM_cts_ind.dsm_cts_ind_processing>`. 
Potentials relevant for the high and extra-high voltage grid are identified in the function :func:`dsm_cts_ind<egon.data.datasets.DSM_cts_ind.dsm_cts_ind>`, 
potentials within the medium- and low-voltage grids are determined within the function :func:`dsm_cts_ind_individual<egon.data.datasets.DSM_cts_ind.dsm_cts_ind_individual>` 
in a higher spatial resolution. All this is part of the dataset :py:class:`DsmPotential <egon.data.datasets.DsmPotential>`.

Loads eligible to be shifted are assumed within industrial loads and loads from Commercial, Trade and Service (CTS). 
Therefore, load time series from these sectors are used as input data (see section ref:`elec_demand-ref`).
Shiftable shares of loads mainly derive from heating and cooling processes and selected energy-intensive 
industrial processes (cement production, wood pulp, paper production, recycling paper). Technical and sociotechnical 
constraints are considered using the parametrization elaborated in [Heitkoetter]_. An overview over the 
resulting potentials can be seen in figure :ref:`dsm_potential`. The table below summarizes the aggregated potential 
for Germany per scenario. 

.. figure:: /images/DSM_potential.png
  :name: dsm_potential
  :width: 600 
  
  Aggregated DSM potential in Germany for scenario ``eGon2035``
  
.. list-table:: Aggregated DSM Potential for Germany
   :widths: 20 20 20
   :header-rows: 1

   * - 
     - CTS
     - Industry

   * - eGon2035
     - 1.2 GW
     - 150 MW

   * - eGon100RE
     - 900 MW
     - 150 MW

DSM is modelled following the approach of [Kleinhans]_. DSM components are created wherever 
respective loads are seen. Minimum and maximum shiftable power per time step depict time-dependent 
charging and discharging power of a storage-equivalent buffers. Time-dependent capacities 
of those buffers account for the time frame of management bounding the period within which 
the shifting can be conducted. Figure :ref:`dsm_shifted_p-example` shows the resulting potential at one exemplary bus.

.. figure:: /images/shifted_dsm-example.png
  :name: dsm_shifted_p-example
  :width: 600 
  
  Time-dependent DSM potential at one exemplary bus

 

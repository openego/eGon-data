Battery storage units comprise home batteries and larger, grid-supportive batteries. National capacities for home batteries arise from external sources, e.g. the Grid Development Plan for the scenario ``eGon2035``, whereas the capacities of large-scale batteries are a result of the grid optimization tool `eTraGo <https://github.com/openego/eTraGo>`_.

Home battery capacities are first distributed to medium-voltage grid districts (MVGD) and based on that further disaggregated to single buildings. The distribution on MVGD level is done proportional to the installed capacities of solar rooftop power plants, assuming that they are used as solar home storage. 

Potential large-scale batteries are included in the data model at every substation. The data model includes technical and economic parameters, such as efficiencies and investment costs. The energy-to-power ratio is set to a fixed value of 6 hours. Other central parameters are given in the following table

.. list-table:: Parameters of batteries for scenario eGon2035
   :widths: 40 30 30
   :header-rows: 1

   * - 
     - Value
     - Sources

   * - Efficiency store
     - 98 % 
     - [DAE_store]_

   * - Efficiency dispatch
     - 98 %
     - [DAE_store]_
     
   * - Standing loss
     - 0 %
     - [DAE_store]_
     
   * - Investment costs
     - 838 €/kW
     - [DAE_store]_

   * - Home storage units
     - 16.8 GW
     - [NEP2021]_


On transmission grid level, distinguishing between home batteries and large-scale batteries was not possible. Therefore, the capacities of home batteries were set as a lower boundary of the large-scale battery capacities. 
This is implemented in the dataset :py:class:`StorageEtrago <egon.data.datasets.storages_etrago.StorageEtrago>`, the data for batteries in the transmission grid is stored in the database table :py:class:`grid.egon_etrago_storage <egon.data.datasets.etrago_setup.EgonPfHvStorage>`.

The heat sector can provide flexibility through stores that allow shifting energy in time. The data model includes hot water tanks as heat stores in individual buildings and pit thermal energy storage for district heating grids (further described in :ref:`district-heating`). 

Within the data model, potential locations as well as technical and economic parameters of these stores are defined. The installed store and (dis-)charging capacities are part of the grid optimization methods that can be performed by `eTraGo <https://github.com/openego/eTraGo>`_. The power-to-energy ratio is not predefined but a result of the optimization, which allows to build heat stores with various time horizons. 

Individual heat stores can be built in every building with an individual heat pump.  Central heat stores can be built next to district heating grids. There are no maximum limits for the energy output as well as (dis-)charging capacities implemented yet.

Central cost assumptions for central and decentral heat stores are listed in the table below. The parameters can differ for each scenario in order to include technology updates and learning curves. The table focuses on the scenario ``eGon2035``.

.. list-table:: Parameters of heat stores
   :widths: 16 16 16 16 16 16
   :header-rows: 1

   * - 
     - Technology
     - Costs for store capacity
     - Costs for (dis-)charging capacity
     - Round-trip efficiency
     - Sources

   * - District heating 
     - Pit thermal energy storage
     - 0.51 EUR / kWh
     - 0 EUR / kW
     - 70 % 
     - [DAE_store]_

   * - Buildings with heat pump
     - Water tank
     - 1.84 EUR / kWh
     - 0 EUR / kW
     - 70 % 
     - [DAE_store]_

The heat stores are implemented as a part of the dataset :py:class:`HeatEtrago <egon.data.datasets.heat_etrago.HeatEtrago>`, the data is written into the tables :py:class:`grid.egon_etrago_bus <egon.data.datasets.etrago_setup.EgonPfHvBus>`, :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` and :py:class:`grid.egon_etrago_store <egon.data.datasets.etrago_setup.EgonPfHvStore>`.


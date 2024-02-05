****
Data
****
The description of the methods, input data and results of the eGon-data pipeline is given in the following section.
References to datasets and functions are integrated if more detailed information is required.

Main input data and their processing
====================================

All methods in the eGon-data workflow rely on public and freely available data from different external sources. The most important data sources
and their processing within the eGon-data pipeline are described here.

.. include:: data/input_data.rst

Grid models
===========

Power grid models of different voltage levels form a central part of the eGon-data model, which is required for cross-grid-level optimization.
In addition, sector coupling necessitates the representation of the gas grid infrastructure, which is also described in this section.

Electricity grid
----------------

.. include:: data/electricity_grids.rst

Gas grid
--------

.. include:: data/gas_grids.rst

Demand
======

Electricity, heat and gas demands from different consumption sectors are taken into account in eGon-data. The related methods to distribute and
process the demand data are described in the following chapters for the different consumption sectors separately.

.. _electricity-demand-ref:

Electricity
-----------

.. include:: data/electricity_demand.rst

.. _heat-demand-ref:

Heat
----

.. include:: data/heat_demand.rst

Gas
---

.. include:: data/gas_demand.rst

.. _mobility-demand-ref:

Mobility
--------

.. include:: data/mobility_demand.rst


Supply
======

The distribution and assignment of supply capacities or potentials are carried out technology-specific. The different methods are described in the
following chapters.

Electricity
-----------

.. include:: data/electricity_supply.rst

Heat
----

.. include:: data/heat_supply.rst

Gas
---
.. include:: data/gas_supply.rst


Flexibility options
===================

Different flexibility options are part of the model and can be utilized in the optimization of the energy system. Therefore detailed information about
flexibility potentials and their distribution are needed. The considered technologies described in the following chapters range from different storage units,
through dynamic line rating to Demand-Side-Management measures.

Demand-Side-Management
----------------------

.. include:: data/DSM.rst

Dynamic line rating
-------------------

.. include:: data/DLR.rst

.. _flexible-charging-ref:

Flexible charging of EVs
---------------------------

.. include:: data/e-mobility.rst

Battery stores
----------------

.. include:: data/batteries.rst

Gas stores
-----------------

.. include:: data/gas_stores.rst

Heat stores
-------------

.. include:: data/heat_stores.rst


Published data
==============



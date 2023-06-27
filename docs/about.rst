***************
About eGon-data
***************

Project background
==================

egon-data provides a transparent and reproducible open data-based data processing pipeline for generating data models suitable for energy system modeling. The data is customized for the requirements of the research project eGo_n. The research project aims to develop tools for open and cross-sectoral planning of transmission and distribution grids. For further information please visit the `eGo_n project website <https://ego-n.org/>`_.
egon-data is a further development of the `Data processing <https://github.com/openego/data_processing>`_ developed in the former research project `open_eGo <https://openegoproject.wordpress.com/>`_. It aims to extend the data models as well as improve the replicability and manageability of the data preparation and processing. 
The resulting data set serves as an input for the optimization tools `eTraGo <https://github.com/openego/eTraGo>`_, `ding0 <https://github.com/openego/ding0>`_ and `eDisGo <https://github.com/openego/eDisGo>`_ and delivers, for example, data on grid topologies, demands/demand curves and generation capacities in a high spatial resolution. The outputs of egon-data are published under open-source and open-data licenses.  


Objectives of the project
=========================

Driven by the expansion of renewable generation capacity and the progressing electrification of other energy sectors, the electrical grid increasingly faces new challenges: fluctuating supply of renewable energy and simultaneously a changing demand pattern caused by sector coupling. However, the integration of non-electric sectors such as gas, heat, and e-mobility enables more flexibility options. The eGo_n project aims to investigate the effects of sector coupling on the electrical grid and the benefits of new flexibility options. This requires the creation of a spatially and temporally highly resolved database for all sectors considered. 

eGon-data as one element of the eGo-Toolchain
=============================================

In the eGo_n project different tools were developed, which are in exchange with each other and have to serve the respective requirements on data scope, resolution, and format. The results of the data model creation have to be especially adapted to the requirements of the tools eTraGo and eDisGo for power grid optimization on different grid levels. 
A PostgreSQL database serves as an interface between the data model creation and the optimization tools.
The figure below visualizes the interdependencies between the different tools.  


Modeling concept and scenarios
===============================





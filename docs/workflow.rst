********
Workflow
********

Project background
-----------------

egon-data provides a transparent and reproducible open data based data processing pipeline for generating data models suitable for energy system modeling. The data is customized for the requirements of the research project eGo_n. The research project aims to develop tools for an open and cross-sectoral planning of transmission and distribution grids. For further information please visit the `eGo_n project website <https://ego-n.org/>`_.
egon-data is a further development of the `Data processing <https://github.com/openego/data_processing>`_ developed in the former research project `open_eGo <https://openegoproject.wordpress.com/>`_. It aims for an extensions of the data models as well as for a better replicability and manageability of the data preparation and processing. 
The resulting data set serves as an input for the optimization tools `eTraGo <https://github.com/openego/eTraGo>`_, `ding0 <https://github.com/openego/ding0>`_ and `eDisGo <https://github.com/openego/eDisGo>`_ and delivers for example data on grid topologies, demands/demand curves and generation capacities in a high spatial resolution. The outputs of egon-data are published under open source and open data licenses.  

Data
----

egon-data retrieves and processes data from several different external input sources which are all freely available and published under an open data license. The process handles data with different data types, such as spatial data with a high geographical resolution or load/generation time series with an hourly resolution.  

Execution
---------

In principle egon-data is not limited to the use of a specific programming language as the workflow integrates different scripts using Apache Airflow, but Python and SQL are widely used within the process. Apache Airflow organizes the order of execution of processing steps through so-called operators. In the default case the SQL processing is executed on a containerized local PostgreSQL database using Docker. For further information on Docker and its installation please refer to their `documentation <https://docs.docker.com/>`_. Connection information of our local Docker database are defined in the corresponding `docker-compose.yml <https://github.com/openego/eGon-data/blob/dev/src/egon/data/airflow/docker-compose.yml>`_ 

The egon-data workflow is composed of four different sections: database setup, data import, data processing and data export to the OpenEnergy Platform. Each section consists of different tasks, which are managed by Apache Airflow and correspond with the local database. 
Only final datasets which function as an input for the optimization tools or selected interim results are uploaded to the `Open Energy Platform <https://openenergy-platform.org/>`_. 
The data processing in egon-data needs to be performed locally as calculations on the Open Energy Platform are prohibited. 
More information on how to run the workflow can be found in the `getting started section <https://egon-data.readthedocs.io/en/latest/getting_started.html#run-the-workflow>`_ of our documentation.

.. _DP_workflow_sketch:
.. figure:: images/DP_Workflow_15012021.svg
 

Versioning
----------

.. warning::
   Please note, the following is not implemented yet, but we are working on it.

Source code and data are versioned independendly from each other. Every data table uploaded to the Open Energy Platform contains a column 'version' which is used to identify different versions of the same data set. The version number is maintained for every table separately. This is a major difference to the versioning concept applied in the former data processing where all (interim) results were versioned under the same version number.  









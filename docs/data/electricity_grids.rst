.. _ehv-hv-grids:

High and extra-high voltage grids
++++++++++++++++++++++++++++++++++

The model of the German extra-high (eHV) and high voltage (HV) grid is based
on data retrieved from OpenStreetMap (status January 2021) [OSM]_ and additional
parameters for standard transmission lines from [Brakelmann2004]_. To gather all
required information, such as line topology, voltage level, substation locations,
and electrical parameters, to create a calculable power system model, the `osmTGmod
tool <https://github.com/openego/osmTGmod>`_ was used. The corresponding dataset
:py:class:`Osmtgmod <egon.data.datasets..osmtgmod.Osmtgmod>` executes osmTGmod
and writes the resulting data to the database.

The resulting grid model includes the voltage levels 380, 220 and 110 kV and
all substations interconnecting these grid levels. For further information on the
generation of the grid topology please refer to [Mueller2018]_.

.. _ding0-grids:

Medium and low-voltage grids
++++++++++++++++++++++++++++

Medium (MV) and low (LV) voltage grid topologies for entire Germany are generated using
the python tool ding0 `ding0 <https://github.com/openego/ding0>`_.
ding0 generates synthetic grid topologies based on high-resolution geodata and routing
algorithms as well as typical network planning principles.
The generation of the
grid topologies is not part of eGon_data, but ding0 solely uses data generated with eGon_data,
such as locations of HV/MV stations (see :ref:`ehv-hv-grids`), locations and peak demands
of buildings in the grid (see :ref:`building-data-ref` respectively :ref:`electricity-demand-ref`),
as well as locations of generators from MaStR (see :ref:`mastr-ref`). A full list
of tables used in ding0 can be found in its `config <https://github.com/openego/ding0/blob/dev/ding0/config/config_db_tables.cfg>`_.
An exemplary MV grid with one underlying LV grid is shown in figure :ref:`ding0-mv-grid`.
The grid data of all over 3.800 MV grids is published on `zenodo <https://zenodo.org/record/890479>`_.

.. figure:: /images/ding0_mv_lv_grid.png
  :name: ding0-mv-grid
  :width: 600

  Exemplary synthetic medium-voltage grid with underlying low-voltage grid generated with ding0

Besides data on buildings and generators, ding0 requires data on the supplied areas
by each grid. This is as well done in eGon_data and described in the following.

.. _mv-grid-districts:

MV grid districts
~~~~~~~~~~~~~~~~~~

Medium-voltage (MV) grid districts describe the area supplied by one MV grid.
They are defined by one polygon that represents the
supply area. Each MV grid district is connected to the HV grid via a single
substation. An exemplary MV grid district is shown in figure :ref:`ding0-mv-grid` (orange line).

The MV grid districts are generated in the dataset
:class:`MvGridDistricts<egon.data.datasets.mv_grid_districts.MvGridDistricts>`.
The methods used for identifying the MV grid districts are heavily inspired
by Hülk et al. (2017) [Huelk2017]_
(section 2.3), but the implementation differs in detail.
The main difference is that direct adjacency is preferred over proximity.
For polygons of municipalities
without a substation inside, it is iteratively checked for direct adjacent
other polygons that have a substation inside. Speaking visually, a MV grid
district grows around a polygon with a substation inside.

The grid districts are identified using three data sources

1. Polygons of municipalities (:class:`Vg250GemClean<egon.data.datasets.mv_grid_districts.Vg250GemClean>`)
2. Locations of HV-MV substations (:class:`EgonHvmvSubstation<egon.data.datasets.osmtgmod.substation.EgonHvmvSubstation>`)
3. HV-MV substation voronoi polygons (:class:`EgonHvmvSubstationVoronoi<egon.data.datasets.substation_voronoi.EgonHvmvSubstationVoronoi>`)

Fundamentally, it is assumed that grid districts (supply areas) often go
along borders of administrative units, in particular along the borders of
municipalities due to the concession levy.
Furthermore, it is assumed that one grid district is supplied via a single
substation and that locations of substations and grid districts are designed
for aiming least lengths of grid line and cables.

With these assumptions, the three data sources from above are processed as
follows:

* Find the number of substations inside each municipality
* Split municipalities with more than one substation inside

  * Cut polygons of municipalities with voronoi polygons of respective
    substations
  * Assign resulting municipality polygon fragments to nearest substation
* Assign municipalities without a single substation to nearest substation in
  the neighborhood
* Merge all municipality polygons and parts of municipality polygons to a
  single polygon grouped by the assigned substation

For finding the nearest substation, as already said, direct adjacency is
preferred over closest distance. This means, the nearest substation does not
necessarily have to be the closest substation in the sense of beeline distance.
But it is the substation definitely located in a neighboring polygon. This
prevents the algorithm to find solutions where a MV grid districts consists of
multi-polygons with some space in between.
Nevertheless, beeline distance still plays an important role, as the algorithm
acts in two steps

1. Iteratively look for neighboring polygons until there are no further
   polygons
2. Find a polygon to assign to by minimum beeline distance

The second step is required in order to cover edge cases, such as islands.

For understanding how this is implemented into separate functions, please
see :func:`define_mv_grid_districts<egon.data.datasets.mv_grid_districts.define_mv_grid_districts>`.

.. _load-areas-ref:

Load areas
~~~~~~~~~~~~

Load areas (LAs) are defined as geographic clusters where electricity is consumed.
They are used in ding0 to determine the extent and number of LV grids. Thus, within
each LA there are one or multiple MV-LV substations, each supplying one LV grid.
Exemplary load areas are shown in figure :ref:`ding0-mv-grid` (grey and orange areas).

The load areas are set up in the
:class:`LoadArea<egon.data.datasets.loadarea.LoadArea>` dataset.
The methods used for identifying the load areas are heavily inspired
by Hülk et al. (2017) [Huelk2017]_ (section 2.4).

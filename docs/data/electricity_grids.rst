Information about our electricity grids and how they were created

High and extra-high voltage grids 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The model of the German extra-high (eHV) and high voltage (HV) grid is based 
on data retrieved from OpenStreetMap (status January 2021) [OSM]_ and additional 
parameters for standard transmission lines from [Brakelmann2004]_. To gather all 
required information, such as line topology, voltage level, substation locations, 
and electrical parameters, to create a calculable power system model, the `osmTGmod 
tool <https://github.com/openego/osmTGmod>`_was used. The corresponding dataset 
:py:class:`Osmtgmod <egon.data.datasets..osmtgmod.Osmtgmod>` executes osmTGmod 
and writes the resulting data to the database.

The resulting grid model includes the voltage levels 380, 220 and 110 kV and
all substations interconnecting these grid levels. For further information on the 
generation of the grid topology please refer to [Mueller2018]_.

MV grid districts
~~~~~~~~~~~~~~~~~
Medium-voltage (MV) grid districts describe the area supplied by one MV grid.
They are defined by one polygon that represents the
supply area. Each MV grid district is connected to the HV grid via a single
substation.

The MV grid districts are generated in the module
:py:mod:`mv_grid_districts<egon.data.datasets.mv_grid_districts>`.
The methods used for identifying the MV grid districts are heavily inspired
by `Hülk et al. (2017)
<https://somaesthetics.aau.dk/index.php/sepm/article/view/1833/1531>`_
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

Medium and low-voltage grids
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

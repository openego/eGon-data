To calculate the transmission capacity of each transmission line in the model,
the procedure suggested in the **Principles for the Expansion Planning of the
German Transmission Network** [NEP2021a]_ where used:

1. Import the temperature and wind temporal raster layers from ERA-5. Hourly
resolution data from the year 2011 was used. Raster resolution
latitude-longitude grids at 0.25° x 0.25°.

2. Import shape file for the 9 regions proposed by the Principles for
the Expansion Planning. See Figure 1.

.. image:: images/regions_DLR.png
  :width: 400
  :alt: regions DLR

Figure 1: Representative regions in Germany for DLR analysis [NEP2021a]_

3. Find the lowest wind speed in each region. To perform this, for each
independent region, the wind speed of every cell in the raster layer should be
extracted and compared. This procedure is repeated for each hour in the
year 2011. The results are the 8760 lowest wind speed per region.

4. Find the highest temperature in each region. To perform this, for each
independent region, the temperature of every cell in the raster layer should
be extracted and compared. This procedure is repeated for each hour in the
year 2011. The results are the 8760 maximum temperature per region.

5. Calculate the maximum capacity for each region using the parameters shown in
Figure 2.

.. image:: images/table_max_capacity_DLR.png
  :width: 400
  :alt: table_max_capacity_DLR

Figure 2: transmission capacity based on max temperature and min wind speed [NEP2021a]_

6. Assign the maximum capacity of the corresponding region to each transmission
line inside each one of them. Crossborder lines and underground lines receive
no values. It means that their capacities are static and equal to their nominal
values. Lines that cross borders between regions receive the lowest
capacity per hour of the regions containing the line.

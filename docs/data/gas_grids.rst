The gas grid data stems from the *SciGRID_gas project* (https://www.gas.scigrid.de/) which covers the European Gas
Transmission Grid. All data generated in the *SciGRID_gas* project is licenced under 
*Creative Commons Attribution 4.0 International Public License*.
The specific dataset version is IGGIELGN and can be downloaded at https://zenodo.org/record/4767098.
*SciGRID_gas* contains extensive data on pipelines, storages, LNGs, productions, consumers and more. 
Further information can be obtained in the IGGIELGN documentation.
For eGon-data, *SciGRID_gas* infrastructure data in Germany has been extracted and used in full resolution
while data of neighboring countries has been aggregated.


Methane grid 
~~~~~~~~~~~~
In the eGon2035 scenario the methane grid is, apart from minor adjustments, equivalent to the gas grid described in the 
*SciGRID_gas IGGIELGN* dataset.

Hydrogen grid
~~~~~~~~~~~~~
In the eGon2035 scenario H2 nodes are present at every methane bus (H2_grid) and at locations where there
are possible H2 cavern storages available (H2_saltcavern). There is no explicit H2 pipeline grid available but H2 can 
be transported using the methane grid. 

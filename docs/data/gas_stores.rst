Hydrogen stores
---------------
There are two types of hydrogen stores available: Underground respectively saltcavern stores 
and overground stores respectively steel tank stores. The steel tank stores are available at every hydrogen bus 
and have no restrictions regarding the possible build-up potential. On the other hand saltcavern stores are capped 
by the respective methane underground storage capacity stemming from the *SciGRID_gas IGGIELGN* dataset.

Methane stores
--------------
The data of the methane stores stems from the *SciGRID_gas IGGIELGN* dataset picturing the exisiting CH4 cavern stores. 
Additionally the CH4 grid has a storage capacity of 130 GWh which is an estimation of the Bundesnetzagentur. For the 
scenario eGon100RE this storage capacity is split between H2 and CH4 stores, with the same share as the pipes capacity.
These capacities have been calculated with a *PyPSA-eur-sec* run.
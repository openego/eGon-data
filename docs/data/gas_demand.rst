In the scenario eGon2035 the gas demand data in Germany stems from the eXtremOS project 
(https://openaccess.ffe.de/10.34805/ffe-24-21/) where demands are 
provided on a NUTS-3 level and hourly resolution for the year 2035. 
These include methane and hydrogen demands for industry. 
For the neighboring countries there are no hydrogen nodes available. Instead respective 
hydrogen demands for industry are indirectly modelled as electricity demands. The total 
hydrogen demand is derived from the *Distributed Energy Scenario* of the *TYNDP 2020* and 
has been linearly interpolated between the years 2030 and 2040. The demands are temporarily 
constant. 
The methane demands of neighboring countries accounts for industry demands and 
heating demands. The total demand data stems also from the *Distributed Energy Scenario* of the *TYNDP 2020*
interpolated between 2030 and 2040 while ites temporal profile is derived from the *PyPSA-eur-sec* run 
because of the high share of rural heating in total methane demand. 

For the scenario eGon100RE the methane and hydrogen demands for industry in Germany 
have been calculated in a *PyPSA-eur-sec* run. The spatial and temporal distribution 
accords to the hydrogen industry demands of the eXtremOS project for the year 2050. 
In eGon100RE no industrial methane demand is assumed.
For the neighboring countries the industrial gas demands (methane and hydrogen) stem from 
the *PyPSA-eur-sec* run.
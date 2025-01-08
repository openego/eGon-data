# -*- coding: utf-8 -*-
# SPDX-FileCopyrightText: : 2023- The PyPSA-Eur Authors
#
# SPDX-License-Identifier: MIT

from pypsa.optimization.compat import get_var, define_constraints, linexpr

def custom_extra_functionality(n, snapshots, snakemake):
    """
    Add custom extra functionality constraints.
    """
    n.model.constraints.remove("Generator-e_sum_max")
    

    #if n.meta["wildcards"]['planning_horizons'] == "2045":
    #    min_wind_onshore = 100e6
    #    min_solar = 200e6
    #    min_wind_offshore = 50e6
    if n.meta["wildcards"]['planning_horizons'] == "2035":
        min_wind_onshore = 111309
        min_solar = 232228
        min_wind_offshore = 39122
    elif n.meta["wildcards"]['planning_horizons'] == "2030":
        min_wind_onshore = 86583
        min_solar = 165173
        min_wind_offshore = 27239
    elif n.meta["wildcards"]['planning_horizons'] == "2025":
        min_wind_onshore = 61856
        min_solar = 98119
        min_wind_offshore = 15356
    
    wind_offshore_ext = list(
        n.generators.index[
            (n.generators.index.str.contains("offwind")
             & (n.generators.bus.str.contains("DE"))
             & (n.generators.p_nom_extendable)
             )
        ]
    )
    
    wind_offshore_fixed = n.generators[
            (n.generators.index.str.contains("offwind")
             & (n.generators.bus.str.contains("DE"))
             & (n.generators.p_nom_extendable==False)
             )
        ].p_nom.sum()
    
    
    wind_onshore_ext = list(
        n.generators.index[
            (n.generators.index.str.contains("onwind")
             & (n.generators.bus.str.contains("DE")))
            & (n.generators.p_nom_extendable)
        ]
    )
    
    wind_onshore_fixed = n.generators[
            (n.generators.index.str.contains("onwind")
             & (n.generators.bus.str.contains("DE"))
             & (n.generators.p_nom_extendable==False)
             )
        ].p_nom.sum()
    
    
    solar_ext = list(
        n.generators.index[
            (n.generators.carrier.isin(["solar", "solar-hst", "solar rooftop"])
             & (n.generators.bus.str.contains("DE"))
             & (n.generators.p_nom_extendable))
        ]
    )
    
    solar_fixed = n.generators[
            (n.generators.carrier.isin(["solar", "solar-hst", "solar rooftop"])
             & (n.generators.bus.str.contains("DE"))
             & (n.generators.p_nom_extendable==False)
             )
        ].p_nom.sum()
    
    if n.meta["wildcards"]['planning_horizons'] == "2045":
        min_wind_onshore = wind_onshore_fixed
        min_solar = solar_fixed
        min_wind_offshore = wind_offshore_fixed


    define_constraints(n, 
                       get_var(n, "Generator", "p_nom").loc[wind_offshore_ext].sum() ,
                       ">=",
                       min_wind_offshore - wind_offshore_fixed ,
                       "Global",
                       "min_offwind_de"
                       )
    
    define_constraints(n, 
                       get_var(n, "Generator", "p_nom").loc[wind_onshore_ext].sum() ,
                       ">=",
                       min_wind_onshore - wind_onshore_fixed,
                       "Global",
                       "min_onwind_de"
                       )
    
    define_constraints(n, 
                       get_var(n, "Generator", "p_nom").loc[solar_ext].sum() ,
                       ">=",
                       min_solar - solar_fixed,
                       "Global",
                       "min_solar_de"
                       )
    

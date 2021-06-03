"""The module containing all code dealing with geothermal potentials and costs

"""
import geopandas as gpd
import pandas as pd
import numpy as np

def calc_geothermal_potentials():
    # Set parameters
    ## specific thermal capacity of water in kJ/kg*K (p. 95)
    c_p = 4
    ## full load hours per year in h (p. 95)
    flh = 3000
    ## mass flow per reservoir in kg/s (p. 95)
    m_flow = pd.Series(
        data={'NDB': 35, 'ORG': 90, 'SMB': 125}, name = 'm_flow')
    ## production wells per km^2 (p. 95)
    pw_km = 0.25
    ## geothermal potentials per temperature (p. 94)
    potentials = gpd.read_file(
        'geothermal_potential/geothermal_potential_germany.shp')
    ## temperature heating system in °C (p. 95)
    sys_temp = 60
    ## temeprature losses heat recuperator in °C (p. 95)
    loss_temp = 5

    # calc mean temperatures per region (p. 93/94):
    # "Die mittlere Reservoirtemperatur für diese Zonen wird ermittelt, indem
    # pauschal 15 K auf die angegebene untere Temperaturgrenze addiert werden."
    potentials['mean_temperature'] = potentials['min_temper'] + 15

    # exclude regions with mean_temp < 60°C (p. 93):
    # "Für eine direkte Nutzung hydrothermaler Reservoire werden in der Analyse
    # nur jene Wärmenetzzellen berücksichtigt, die in Temperaturzonen mit mehr
    # als 60 °C im GeotIS-Datensatz liegen."
    potentials = potentials[potentials.mean_temperature>=60]

    # exclude regions outside of NDB, ORG or SMB because of missing mass flow
    potentials = potentials[~potentials.reservoir.isnull()]

    # use only regions near diostrict heating grid
    # TODO: implement this when district heating geometrys are available

    # calculate potential heat flows per district heating grid  (p. 95)
    ## Q = m_flow * c_p * (reservoir_temp - loss_temp - sys_temp) * flh
    # TODO: change structure when district heating geometrys are available
    ## set mass flows per region
    potentials['m_flow'] = potentials.join(m_flow, on = 'reservoir').m_flow

    # calculate flow in kW
    potentials['Q_flow'] = potentials.m_flow * c_p * (
        potentials.mean_temperature - loss_temp - sys_temp)

    potentials['Q'] = potentials.Q_flow * flh

    return potentials

def calc_geothermal_costs(max_costs=np.inf):
    # Set parameters
    ## drilling depth per reservoir in m (p. 99)
    depth = pd.Series(
        data={'NDB': 2500, 'ORG': 3400, 'SMB': 2800}, name = 'depth')
    ## drillings costs in EUR/m (p. 99)
    depth_costs = 1500
    ## ratio of investment costs to drilling costs  (p. 99)
    ratio = 1.4
    ## annulazaion factors
    p = 0.045
    T = 30
    PVA = 1/p - 1/(p*(1+p)**T)
    ## production wells per km^2 (p. 95)
    pw_km = 0.25

    # calculate overnight investment costs per drilling and region
    overnight_investment = depth*depth_costs*ratio
    investment_per_year = overnight_investment/PVA

    # investment costs per well according to p.99
    costs = pd.Series(
        data={'NDB': 12.5e6, 'ORG': 17e6, 'SMB': 14e6}, name = 'costs')

    potentials = calc_geothermal_potentials()


    potentials['cost_per_well'] = potentials.join(
        costs, on = 'reservoir').costs

    potentials['cost_per_well_mw'] = potentials.cost_per_well/1000/potentials.Q_flow

    potentials = potentials.to_crs(3035)

    # area weighted mean costs per well and mw
    np.average(potentials.cost_per_well_mw, weights=potentials.area)

    return potentials[potentials['cost_per_well_mw']<max_costs]

def plot_costs(potentials):
    import matplotlib.pyplot as plt

    fig, (ax1, ax2) = plt.subplots(1, 2)

    potentials.plot(column='cost_per_well', ax=ax1, legend=True,
                    legend_kwds={'label': "Costs per well"})

    potentials.plot(column='cost_per_well_mw', ax=ax2, legend=True,
                    legend_kwds={'label': "Costs per well and MW"})

    plt.savefig('costs_geothermal.png')
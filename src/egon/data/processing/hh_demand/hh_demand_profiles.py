from egon.data.processing.hh_demand import hh_demand_profiles_tools as hh_tools

if __name__ == "__main__":

    # Create table with mapping of census cells and household elec. profiles
    hh_tools.houseprofiles_in_census_cells()

    # Calculate household electricity demand time series for each MV grid
    profiles_2035 = hh_tools.mv_grid_district_HH_electricity_load(
        "eGon2035",
        2035,
        "0.0.0",
        drop_table=True
    )
    profiles_2050 = hh_tools.mv_grid_district_HH_electricity_load(
        "eGon100RE",
        2050,
        "0.0.0"
    )
    print(profiles_2035)
    print(profiles_2050)

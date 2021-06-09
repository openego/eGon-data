from egon.data.processing.hh_demand import hh_demand_profiles_tools as hh_tools


if __name__ == "__main__":

    # Create table with mapping of census cells and household elec. profiles
    hh_tools.houseprofiles_in_census_cells()

    # Calculate household electricity demand time series for each MV grid
    hh_tools.mv_grid_district_HH_electricity_load()

    # ONLY FOR CHECKING
    # Create table with profiles for each census cell including geom
    hh_tools.mv_grid_district_HH_electricity_load_check()

    # Create table with zensus households including geom from zensus population table
    hh_tools.zensus_household_with_geom_check()

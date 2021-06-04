from egon.data.processing.hh_demand import hh_demand_profiles_tools as hh_tools


if __name__ == "__main__":

    df_profiles = hh_tools.get_household_demand_profiles_raw()
    hh_tools.houseprofiles_in_census_cells()

    df_cell_demand_metadata = hh_tools.get_houseprofiles_in_census_cells()



    import random
    load_area_cell_ids = random.sample(list(df_cell_demand_metadata.index), 100)
    max_value_load_area = hh_tools.get_load_area_max_load(df_profiles, df_cell_demand_metadata, load_area_cell_ids, 2035)
    print(max_value_load_area)
    print(df_cell_demand_metadata.shape)



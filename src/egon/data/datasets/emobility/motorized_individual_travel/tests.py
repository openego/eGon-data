"""
Sanity checks for motorized individual travel
"""

from numpy.testing import assert_allclose

from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    CONFIG_EV,
)


def validate_electric_vehicles_numbers(dataset_name, ev_data, ev_target):
    """Validate cumulative numbers of electric vehicles' distribution.

    Tests
    * Check if all cells are not NaN
    * Check if total number matches produced results (tolerance: 0.01 %)

    Parameters
    ----------
    dataset_name : str
        Name of data, used for error printing
    ev_data : pd.DataFrame
        EV data
    ev_target : int
        Desired number of EVs
    """
    assert not ev_data.isna().any().any()

    assert_allclose(
        ev_data[[_ for _ in CONFIG_EV.keys()]].sum().sum(),
        ev_target,
        rtol=0.0001,
        err_msg=f"Dataset on EV numbers [{dataset_name}] seems to be flawed.",
    )

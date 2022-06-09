"""
Sanity checks for motorized individual travel
"""

from numpy.testing import assert_allclose
import pytest

from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    CONFIG_EV,
)


@pytest.mark.skip(
    reason="Can only be tested with eGon database."
)
def test_ev_numbers(dataset_name, ev_data, ev_target):
    """Validate cumulative numbers of electric vehicles' distribution

    Tests
    * Check if all cells are not nan
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
    assert ev_data.isna().any().any() is False

    assert_allclose(
        ev_data[[_ for _ in CONFIG_EV.keys()]].sum().sum(),
        ev_target,
        rtol=0.0001,
        err_msg=f"Dataset on EV numbers [{dataset_name}] seems to be flawed.",
    )

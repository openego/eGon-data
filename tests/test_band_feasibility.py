"""The two-path band test against an LP solver on random stores."""

import numpy as np
import pytest
from scipy import sparse
from scipy.optimize import linprog

from egon.data.validation.rules.custom.sanity.band_feasibility import (
    band_gaps,
)

TOL = 1e-7


def lp_feasible(e_initial, lower, upper, driving, capacity):
    """Whether a charging schedule follows the band (HiGHS)."""
    n = len(lower)
    # Variables [p_0..p_{n-1}, e_0..e_{n-1}]:
    # e_t - e_{t-1} - p_t = -driving_t, e_{-1} = e_initial.
    eye = sparse.identity(n, format="csr")
    shift = sparse.eye(n, k=-1, format="csr")
    a_eq = sparse.hstack([-eye, eye - shift], format="csr")
    b_eq = -np.asarray(driving, dtype=float)
    b_eq[0] += e_initial
    bounds = np.column_stack(
        [np.r_[np.zeros(n), lower], np.r_[capacity, upper]]
    )
    result = linprog(
        np.zeros(2 * n), A_eq=a_eq, b_eq=b_eq, bounds=bounds, method="highs"
    )
    assert result.status in (0, 2), result.message
    return result.status == 0


def random_store(rng, n):
    """A random band around a feasible trajectory, sometimes tightened."""
    capacity = rng.uniform(0, 2, n) * (rng.random(n) < 0.6)
    charge = capacity * rng.random(n)
    driving = rng.uniform(0, 1.5, n) * (rng.random(n) < 0.4)
    e_initial = rng.uniform(5, 10)
    energy = e_initial + np.cumsum(charge - driving)
    lower = energy - rng.uniform(0, 2, n) * (rng.random(n) < 0.7)
    upper = energy + rng.uniform(0, 2, n) * (rng.random(n) < 0.7)
    # Perturb some bounds, which may or may not make the band infeasible.
    k = rng.integers(0, 4)
    for t in rng.integers(0, n, k):
        if rng.random() < 0.5:
            lower[t] += rng.uniform(0, 3)
        else:
            upper[t] -= rng.uniform(0, 3)
    upper = np.maximum(upper, lower)
    return e_initial, lower, upper, driving, capacity


@pytest.mark.parametrize("seed", range(400))
def test_band_gaps_agrees_with_lp(seed):
    rng = np.random.default_rng(seed)
    store = random_store(rng, int(rng.integers(1, 60)))
    shortfall, excess = band_gaps(*store)
    assert shortfall >= 0 and excess >= 0
    gap = max(shortfall, excess)
    if gap > TOL:
        assert not lp_feasible(*store)
    elif gap == 0:
        assert lp_feasible(*store)


def test_random_stores_cover_both_outcomes():
    outcomes = set()
    for seed in range(400):
        rng = np.random.default_rng(seed)
        store = random_store(rng, int(rng.integers(1, 60)))
        outcomes.add(max(band_gaps(*store)) > TOL)
    assert outcomes == {True, False}


def test_shortfall_and_excess():
    # Needs 3 in one step but can charge only 1: shortfall 2.
    assert band_gaps(5, [5], [10], [3], [1]) == (2.0, 0.0)
    # Must drop to 2 without charging but drives only 1: excess 2.
    assert band_gaps(5, [0], [2], [1], [1]) == (0.0, 2.0)
    # The start of #1596: the deficit is removed twice in step 0.
    assert band_gaps(0.6, [0.6], [1], [0.4], [0.1])[0] == pytest.approx(0.3)
    assert band_gaps(1.0, [0.6], [1], [0.4], [0.1]) == (0.0, 0.0)

"""
Feasibility of an eTraGo flexible charging model's store band.

The M1/N1 and the HGV flex models have the same shape: a bus holding only
a charging link, a ``battery_storage`` store and a driving load. The store
can gain energy only through the link and lose it only by driving, so the
band it has to follow can be checked exactly without a solver, cf.
:func:`band_gaps`. Used by ``MitBandReachable`` and ``HgvBandReachable``.
"""

import numpy as np


def band_gaps(e_initial, lower, upper, driving, capacity):
    """
    Largest shortfall below the lower band and largest excess above the
    upper band of a store, in its energy unit; both 0 if the band is
    feasible.

    Charging as early as possible -- every step as much as ``capacity``
    and the upper band allow -- keeps the store as full as it can be at
    every step, and not charging keeps it as empty as it can be. The band
    is feasible exactly if the first path never falls below the lower band
    and the second never stays above the upper band. After a gap a path
    continues from the violated bound, so later steps are judged on their
    own.

    Parameters
    ----------
    e_initial : float
        Energy before the first step.
    lower, upper : array-like
        Energy bounds at the end of every step.
    driving : array-like
        Energy removed in every step.
    capacity : array-like
        Most energy the link can add in every step (link power times
        efficiency times step length).

    Returns
    -------
    tuple(float, float)
        Shortfall and excess
    """
    lower = np.asarray(lower, dtype=float)
    upper = np.asarray(upper, dtype=float)
    driving = np.asarray(driving, dtype=float)
    capacity = np.asarray(capacity, dtype=float)
    full, empty = float(e_initial), float(e_initial)
    shortfall, excess = 0.0, 0.0
    for t in range(len(lower)):
        full = min(full - driving[t] + capacity[t], upper[t])
        if full < lower[t]:
            shortfall = max(shortfall, lower[t] - full)
            full = lower[t]
        empty = max(empty - driving[t], lower[t])
        if empty > upper[t]:
            excess = max(excess, empty - upper[t])
            empty = upper[t]
    return shortfall, excess

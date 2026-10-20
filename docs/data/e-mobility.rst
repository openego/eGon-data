The flexibility potential of EVs is determined on the basis of the event data
of the vehicle profiles (see :ref:`mobility-demand-mit-ref`).
It is assumed that only charging where the vehicle has reliable access to its
own charging point can be flexibilized. For the new methodology these are the
charging use cases ``depot``, ``home_detached``, ``home_apartment`` and
``work``; ``street``, ``retail``, ``urban_fast`` and ``highway_fast`` are
assumed not to provide demand-side flexibility. The legacy ``eGon2035``
scenario uses its own taxonomy and flexibilizes charging at home and at the
workplace.
Further, vehicle-to-grid is not considered and it is assumed that charging can only be shifted
within a charging event. Shifting charging demand to a later charging event, for example
from charging at work during working hours to charging at home in the evening, is therefore
not possible. In the generation of the event data itself it is already considered, that
EVs are not charged everytime a charging point is available, but only if a certain
lower state of charge (SoC) is reached or the energy level is not sufficient for the next ride.

In `eTraGo <https://github.com/openego/eTraGo>`_, the flexibility of the EVs is modeled
using a storage model based on [Brown2018]_ and [Wulff2020]_.
The used model is visualised in the upper right in figure :ref:`mit-model`.
It is set up for every configured scenario that models flexible charging (cf.
:py:func:`is_flexible<egon.data.datasets.emobility.motorized_individual_travel.model_timeseries.is_flexible>`);
status quo scenarios get a plain load instead.
Each model consists of a bus with a load for the driving demand, a store and a
unidirectional link from the electricity bus. Its maximum charging power per
hour is set to the charging power of the plugged-in vehicles. The stores are
constrained by hourly lower and upper energy limits.

For the new methodology the models are built from **placements**
(:py:func:`generate_placement_models<egon.data.datasets.emobility.motorized_individual_travel.placement_model.generate_placement_models>`):
one district model per MV grid district from its charging locations at voltage
levels 4–7, and one transmission model per grid connection at voltage levels
1–3, which carries the carriers ``BEV_charger_transmission`` and
``land_transport_EV_transmission`` so that it is not counted as part of the
distribution grid. A placement takes its deficit from the store at arrival as
driving load; a flexible placement may recharge it anywhere within its parking
window, an inflexible one along a fixed linear ramp over the parking window.
The store's capacity is the peak battery capacity plugged in at the same time,
and the store starts the year full.

For the legacy methodology the parametrization is conducted per MV grid
district from the vehicles allocated to it, in
:py:func:`generate_load_time_series<egon.data.datasets.emobility.motorized_individual_travel.model_timeseries.generate_load_time_series>`;
the store there represents the fleet's batteries.

Note that the load written for a flexible scenario carries the *driving* energy,
while the one written for its lowflex counterpart and for status quo scenarios
carries the grid-side *charging* energy. Both references are exported
explicitly for the new methodology, together with the recipe for deriving the
realised load shift from an eTraGo result; see
:ref:`mit-reference-points-ref`.

In `eDisGo <https://github.com/openego/eDisGo>`_, the flexibility potential for
controlled charging is modeled using
so-called flexibility bands. These bands comprise an upper and lower power band for
the charging power and an upper and lower energy band for the energy to be recharged
for each charging point in an hourly resolution. These flexibility bands are not
set up in eGon-data but in eDisGo, using the trip data from eGon-data.
For further information on the flexibility bands see
`eDisGo documentation <https://edisgo.readthedocs.io/en/dev/features_in_detail.html#charging-strategies>`_.

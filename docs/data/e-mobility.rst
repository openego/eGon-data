The flexibility potential of  EVs is determined on the basis of the trip data
created with SimBEV (see :ref:`mobility-demand-mit-ref`).
It is assumed, that only charging at private charging points, comprising charging points at
home and at the workplace, can be flexibilized. Public fast (e.g. gas stations) and slow charging (e.g. schools
and shopping facilities) stations are assumed not to provide demand-side flexibility.
Further, vehicle-to-grid is not considered and it is assumed that charging can only be shifted
within a charging event. Shifting charging demand to a later charging event, for example
from charging at work during working hours to charging at home in the evening, is therefore
not possible. In the generation of the trip data itself it is already considered, that
EVs are not charged everytime a charging point is available, but only if a certain
lower state of charge (SoC) is reached or the energy level is not sufficient for the next ride.

In `eTraGo <https://github.com/openego/eTraGo>`_, the flexibility of the EVs is modeled
using a storage model based on [Brown2018]_ and [Wulff2020]_.
The used model is visualised in the upper right in figure :ref:`mit-model`.
Its parametrization is for both the eGon2035 and eGon100RE scenario conducted in the
:py:class:`MotorizedIndividualTravel<egon.data.datasets.emobility.motorized_individual_travel.MotorizedIndividualTravel>`
dataset in the function
:py:func:`generate_load_time_series<egon.data.datasets.emobility.motorized_individual_travel.model_timeseries.generate_load_time_series>`.
The model consists of loads for static driving demands and stores for the fleet’s batteries.
The stores are constrained by hourly lower and upper SoC limits.
The lower SoC limit represents the inflexible charging demand while the
SoC band between the lower and upper SoC limit represents the flexible charging demand.
Further, the charging infrastructure is represented by unidirectional links from electricity
buses to EV buses. Its maximum charging power per hour is set to the available charging power
of grid-connected EVs.

In `eDisGo <https://github.com/openego/eDisGo>`_, the flexibility potential for
controlled charging is modeled using
so-called flexibility bands. These bands comprise an upper and lower power band for
the charging power and an upper and lower energy band for the energy to be recharged
for each charging point in an hourly resolution. These flexibility bands are not
set up in eGon-data but in eDisGo, using the trip data from eGon-data.
For further information on the flexibility bands see
`eDisGo documentation <https://edisgo.readthedocs.io/en/dev/features_in_detail.html#charging-strategies>`_.

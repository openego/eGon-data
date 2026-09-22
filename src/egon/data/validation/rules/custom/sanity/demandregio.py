"""
Sanity check validation rules for DemandRegio demand tables.

Validates the total electricity demand per scenario against the annual demand
target the pipeline scales the data to.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity

from egon.data.datasets.scenario_parameters import get_sector_parameters


class DemandRegioScenarioDemand(DataFrameRule):
    """
    Validate the total demand of a DemandRegio table for one scenario.

    ``insert_hh_demand`` and ``insert_cts_ind_demands`` scale the
    disaggregated demand so that its **national** sum matches
    ``electricity.annual_demand`` of the scenario, and only afterwards
    restrict the result to the configured dataset boundary. The expected
    total therefore depends on the boundary:

    * ``Everything`` -- the national target is met exactly, so the expected
      value is read from the scenario parameters at runtime and no number
      needs to be hard-coded here.
    * any smaller boundary (e.g. ``Schleswig-Holstein``) -- the table holds
      only the NUTS-3 regions inside the boundary, which is an arbitrary
      fraction of the national target. That fraction is not constant across
      scenarios (measured: 3.5 % for households but 1.7-1.9 % for industry),
      so it cannot be derived and has to be supplied via ``expected_total``.

    Pass ``expected_total`` as a
    :func:`~egon.data.validation.resolve_boundary_dependence` mapping with
    ``None`` for ``Everything`` to get both behaviours from one rule.

    A scenario that is absent from the table is **not** an error: the rule
    reports success with severity INFO and an explanatory message, because
    which scenarios a run produces depends on ``--scenarios``.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        sectors,
        expected_total=None,
        rtol: float = 0.01,
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            Target table ("demand.egon_demandregio_hh" or
            "demand.egon_demandregio_cts_ind").
        rule_id : str
            Unique identifier for this validation rule.
        scenario : str
            Scenario to check, e.g. "status2024".
        sectors : Sequence[str]
            Keys of ``electricity.annual_demand`` that make up this table's
            demand: ``["households"]`` for the household table,
            ``["CTS", "industry"]`` for the CTS/industry table.
        expected_total : float, BoundaryDependent or None
            Expected total demand in MWh. ``None`` means "derive the national
            target from the scenario parameters", which is only correct for
            the ``Everything`` boundary.
        rtol : float
            Relative tolerance (default: 0.01 = 1 %).
        """
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            sectors=list(sectors),
            expected_total=expected_total,
            rtol=rtol,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario

    def get_query(self, ctx):
        """Total demand and row count for this scenario.

        The table name is a configured SQL identifier and cannot be bound;
        the scenario is passed as a parameter.
        """
        return f"""
        SELECT
            count(*) AS n_rows,
            sum(demand) AS total_demand
        FROM {self.table}
        WHERE scenario = :scenario
        """

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {"scenario": self.scenario}

    def _skip(self, message):
        """A non-failure result for cases that are legitimately not checkable."""
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=True,
            observed=None,
            expected=None,
            message=message,
            severity=Severity.INFO,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )

    def _target_from_scenario_parameters(self):
        """National annual demand target in MWh, or None if unavailable.

        ``get_sector_parameters`` does not raise a clean error for an unknown
        scenario -- it prints a message and then trips over an unbound local --
        so every exception is treated as "no target available".
        """
        try:
            parameters = get_sector_parameters(
                "electricity", scenario=self.scenario
            )
            annual_demand = parameters["annual_demand"]
            return sum(
                float(annual_demand[sector])
                for sector in self.params.get("sectors", [])
            )
        except Exception:  # noqa: BLE001 - see docstring
            return None

    def evaluate_df(self, df, ctx):
        """Compare the scenario's total demand against the expected value."""
        n_rows = int(df["n_rows"].values[0] or 0)

        # Which scenarios exist depends on --scenarios, so a missing one is
        # reported rather than failed.
        if n_rows == 0:
            return self._skip(
                f"Scenario '{self.scenario}' not present in {self.table}; "
                f"check skipped"
            )

        total = df["total_demand"].values[0]
        if total is None:
            return self._skip(
                f"Scenario '{self.scenario}' has {n_rows} rows in "
                f"{self.table} but no demand values; check skipped"
            )
        observed = float(total)

        expected = self.params.get("expected_total")
        if expected is None:
            expected = self._target_from_scenario_parameters()
            source = "scenario parameters (national target)"
        else:
            source = "configured boundary-dependent value"

        if expected is None:
            return self._skip(
                f"No annual demand target available for scenario "
                f"'{self.scenario}'; check skipped"
            )
        expected = float(expected)

        if expected == 0:
            return self._skip(
                f"Annual demand target for scenario '{self.scenario}' is "
                f"zero; check skipped"
            )

        rtol = float(self.params.get("rtol", 0.01))
        deviation = abs(observed - expected) / abs(expected)
        success = deviation <= rtol

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=observed,
            expected=expected,
            message=(
                f"Scenario '{self.scenario}': total demand "
                f"{observed / 1e6:.3f} TWh vs expected "
                f"{expected / 1e6:.3f} TWh from {source} "
                f"(deviation {deviation * 100:.3f} %, tolerance "
                f"{rtol * 100:.2f} %, {n_rows} rows)"
            ),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )

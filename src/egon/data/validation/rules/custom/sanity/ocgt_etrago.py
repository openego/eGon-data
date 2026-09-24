"""
Sanity check validation rules for the open cycle gas turbine links.

Validates what
:class:`~egon.data.datasets.power_etrago.OpenCycleGasTurbineEtrago` writes
into ``grid.egon_etrago_link``: one ``OCGT`` link per gas power plant of
``supply.egon_power_plants``, from the nearest German CH4 bus (``bus0``) to
the power plant's AC bus (``bus1``), with ``p_nom = el_capacity /
efficiency``.

The neighbouring countries have ``OCGT`` links of their own, written by
another dataset, so the rules only look at links with both buses in
Germany -- the same links the dataset deletes before inserting.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity

import egon.data.config


class _OcgtRule(DataFrameRule):
    """
    Common base for the per-scenario rules of the OCGT links.

    Subclasses write their query on top of the ``ocgt_links`` CTE returned
    by :meth:`_ocgt_links_cte` and return exactly one row, because the
    framework reports an empty query result as a failure.

    A scenario that is absent from the table is **not** an error: the rule
    reports success with severity INFO, because which scenarios a run
    produces depends on ``--scenarios``.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        bus_table: str = "grid.egon_etrago_bus",
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            Link table, "grid.egon_etrago_link".
        rule_id : str
            Unique identifier for this validation rule.
        scenario : str
            Scenario to check, e.g. "status2024".
        bus_table : str
            eTraGo bus table, used to restrict the check to German buses.
        """
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            bus_table=bus_table,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario
        self.bus_table = bus_table

    def _ocgt_links_cte(self):
        """CTE ``ocgt_links``: OCGT links with both buses in Germany.

        ``bus0_carrier`` and ``bus1_carrier`` are kept so rules can check
        the direction of the link.
        """
        return f"""
        ocgt_links AS (
            SELECT
                l.*,
                b0.carrier AS bus0_carrier,
                b1.carrier AS bus1_carrier
            FROM {self.table} l
            JOIN {self.bus_table} b0
                ON b0.bus_id = l.bus0
                AND b0.scn_name = l.scn_name
            JOIN {self.bus_table} b1
                ON b1.bus_id = l.bus1
                AND b1.scn_name = l.scn_name
            WHERE l.scn_name = :scenario
            AND l.carrier = 'OCGT'
            AND b0.country = 'DE'
            AND b1.country = 'DE'
        )
        """

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {"scenario": self.scenario}

    def _result(self, success, message, observed=None, expected=None,
                severity=None):
        """A result with the common fields filled in."""
        if severity is None:
            severity = Severity.INFO if success else Severity.ERROR
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=observed,
            expected=expected,
            message=message,
            severity=severity,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )

    def _skip(self, message):
        """A non-failure result for cases that cannot be checked."""
        return self._result(success=True, message=message)

    def _no_links(self):
        return self._skip(
            f"Scenario '{self.scenario}' has no German OCGT links in "
            f"{self.table}; check skipped"
        )


class OcgtCapacity(_OcgtRule):
    """
    Compare the OCGT links with the gas power plants they are built from.

    Every gas power plant becomes one link from a CH4 bus to an AC bus, and
    ``p_nom`` is the plant's electrical capacity divided by the efficiency.
    So per scenario the number of CH4 -> AC links must equal the number of
    gas plants, and their ``p_nom * efficiency`` must add up to the plants'
    ``el_capacity``. A link that is reversed or points to a bus of another
    carrier is reported separately and missing from the sum.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        power_plants_table: str = "supply.egon_power_plants",
        rtol: float = 1e-6,
        **kwargs,
    ):
        """
        Parameters
        ----------
        power_plants_table : str
            Source table of the gas power plants.
        rtol : float
            Relative tolerance of the capacity (default: 1e-6, i.e.
            rounding only).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            power_plants_table=power_plants_table,
            rtol=rtol,
            **kwargs,
        )
        self.power_plants_table = power_plants_table

    def get_query(self, ctx):
        return f"""
        WITH {self._ocgt_links_cte()},
        power_plants AS (
            SELECT count(*) AS n, sum(el_capacity) AS capacity
            FROM {self.power_plants_table}
            WHERE scenario = :scenario
            AND carrier = 'gas'
        )
        SELECT
            (SELECT n FROM power_plants) AS n_plants,
            (SELECT capacity FROM power_plants) AS plant_capacity,
            count(*) AS n_links,
            count(*) FILTER (
                WHERE bus0_carrier = 'CH4' AND bus1_carrier = 'AC'
            ) AS n_mapped,
            sum(p_nom * efficiency) FILTER (
                WHERE bus0_carrier = 'CH4' AND bus1_carrier = 'AC'
            ) AS link_capacity
        FROM ocgt_links
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        n_plants = int(row["n_plants"] or 0)
        n_links = int(row["n_links"] or 0)
        if n_plants == 0 and n_links == 0:
            return self._skip(
                f"Scenario '{self.scenario}' has neither gas power plants nor "
                f"German OCGT links; check skipped"
            )

        n_mapped = int(row["n_mapped"] or 0)
        expected = float(row["plant_capacity"] or 0)
        observed = float(row["link_capacity"] or 0)
        rtol = float(self.params.get("rtol", 1e-6))

        problems = []
        if n_mapped != n_plants:
            problems.append(
                f"{n_mapped} CH4 -> AC links for {n_plants} gas power plants"
            )
        if n_links != n_mapped:
            problems.append(
                f"{n_links - n_mapped} links not running from a CH4 to an "
                f"AC bus"
            )
        if abs(observed - expected) > rtol * max(abs(expected), 1.0):
            problems.append(
                f"capacity {observed:.1f} MW vs {expected:.1f} MW of the gas "
                f"power plants"
            )

        if problems:
            return self._result(
                success=False,
                observed=observed,
                expected=expected,
                message=(
                    f"Scenario '{self.scenario}': OCGT links do not match "
                    f"{self.power_plants_table}: {'; '.join(problems)}"
                ),
            )
        return self._result(
            success=True,
            observed=observed,
            expected=expected,
            message=(
                f"Scenario '{self.scenario}': {n_links} OCGT links for "
                f"{n_plants} gas power plants, {observed:.1f} MW electrical "
                f"capacity as in {self.power_plants_table}"
            ),
        )


class OcgtPositiveCapacity(_OcgtRule):
    """
    Check that every OCGT link has a positive, finite ``p_nom``.

    A link with zero, negative or NaN capacity can never transport power.
    """

    def get_query(self, ctx):
        return f"""
        WITH {self._ocgt_links_cte()}
        SELECT
            count(*) AS n_links,
            count(*) FILTER (
                WHERE p_nom IS NULL
                OR p_nom = 'NaN'::float8
                OR p_nom <= 0
            ) AS n_invalid
        FROM ocgt_links
        """

    def evaluate_df(self, df, ctx):
        n_links = int(df["n_links"].values[0])
        if n_links == 0:
            return self._no_links()

        n_invalid = int(df["n_invalid"].values[0])
        if n_invalid:
            return self._result(
                success=False,
                observed=n_invalid,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_invalid} of {n_links} "
                    f"OCGT links have a p_nom that is not positive"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_links} OCGT links have "
                f"a positive p_nom"
            ),
        )


class OcgtParameters(_OcgtRule):
    """
    Compare efficiency, marginal cost and extendability with the parameters.

    ``insert_open_cycle_gas_turbines`` sets ``efficiency`` to
    ``gas.efficiency.OCGT`` of the scenario parameters, ``marginal_cost``
    to ``gas.marginal_cost.OCGT / efficiency`` and ``p_nom_extendable`` to
    false. The parameters are read in the rule's own query, so they come
    from the database under validation.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        parameters_table: str = "scenario.egon_scenario_parameters",
        rtol: float = 1e-9,
        **kwargs,
    ):
        """
        Parameters
        ----------
        parameters_table : str
            Scenario parameters table.
        rtol : float
            Relative tolerance against the parameters (default: 1e-9).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            parameters_table=parameters_table,
            rtol=rtol,
            **kwargs,
        )
        self.parameters_table = parameters_table

    def get_query(self, ctx):
        return f"""
        WITH {self._ocgt_links_cte()},
        parameters AS (
            SELECT
                (gas_parameters -> 'efficiency' ->> 'OCGT')::float8
                    AS efficiency,
                (gas_parameters -> 'marginal_cost' ->> 'OCGT')::float8
                    AS marginal_cost
            FROM {self.parameters_table}
            WHERE name = :scenario
        )
        SELECT
            count(l.*) AS n_links,
            (SELECT efficiency FROM parameters) AS efficiency,
            (SELECT marginal_cost FROM parameters) AS marginal_cost,
            min(l.efficiency) AS min_efficiency,
            max(l.efficiency) AS max_efficiency,
            min(l.marginal_cost) AS min_marginal_cost,
            max(l.marginal_cost) AS max_marginal_cost,
            count(*) FILTER (WHERE l.p_nom_extendable) AS n_extendable
        FROM ocgt_links l
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        n_links = int(row["n_links"])
        if n_links == 0:
            return self._no_links()

        efficiency = row["efficiency"]
        cost = row["marginal_cost"]
        if efficiency is None or cost is None or efficiency != efficiency:
            return self._skip(
                f"No OCGT efficiency or marginal cost in the gas parameters "
                f"of scenario '{self.scenario}'; check skipped"
            )
        efficiency = float(efficiency)
        expected_cost = float(cost) / efficiency
        rtol = float(self.params.get("rtol", 1e-9))

        def differs(low, high, expected):
            tolerance = rtol * max(abs(expected), 1.0)
            # NaN compares false, so it has to count as a difference
            return not (
                abs(float(low) - expected) <= tolerance
                and abs(float(high) - expected) <= tolerance
            )

        problems = []
        if differs(row["min_efficiency"], row["max_efficiency"], efficiency):
            problems.append(
                f"efficiency {row['min_efficiency']:g}.."
                f"{row['max_efficiency']:g} vs {efficiency:g}"
            )
        if differs(
            row["min_marginal_cost"], row["max_marginal_cost"], expected_cost
        ):
            problems.append(
                f"marginal cost {row['min_marginal_cost']:g}.."
                f"{row['max_marginal_cost']:g} vs {expected_cost:g}"
            )
        n_extendable = int(row["n_extendable"])
        if n_extendable:
            problems.append(f"{n_extendable} links are extendable")

        if problems:
            return self._result(
                success=False,
                observed=len(problems),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': OCGT links differ from the "
                    f"scenario parameters: {'; '.join(problems)}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_links} OCGT links have "
                f"efficiency {efficiency:g} and marginal cost "
                f"{expected_cost:g} and are not extendable"
            ),
        )


class OcgtNepCapacity(_OcgtRule):
    """
    Compare the OCGT capacity with the NEP list of gas power plants.

    In the future scenarios the gas power plants are the non-CHP gas plants
    of the NEP list, matched to MaStR units by
    ``allocate_conventional_non_chp_power_plants``. A plant that cannot be
    matched -- e.g. a planned plant without location -- is dropped without
    rescaling the others, so the OCGT capacity ends up below the list. The
    rule reports that as a warning, because the loss happens in the power
    plant allocation, not in this dataset.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        nep_scenario: str,
        capacity_column: str,
        nep_table: str = "supply.egon_nep_conventional_powerplants",
        rtol: float = 0.01,
        **kwargs,
    ):
        """
        Parameters
        ----------
        nep_scenario : str
            Scenario tag of the NEP list, "eGon2035" or "reGon".
        capacity_column : str
            Capacity column of the target year, e.g. "c2037_capacity". It
            is read from the row as JSON, so a column that does not exist
            in this run's table yields no reference instead of an error.
        nep_table : str
            NEP list of conventional power plants.
        rtol : float
            Relative tolerance (default: 0.01 = 1 %).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            nep_scenario=nep_scenario,
            capacity_column=capacity_column,
            nep_table=nep_table,
            rtol=rtol,
            **kwargs,
        )
        self.nep_table = nep_table

    def get_query(self, ctx):
        return f"""
        WITH {self._ocgt_links_cte()},
        nep AS (
            SELECT (to_jsonb(n) ->> :capacity_column)::float8 AS capacity
            FROM {self.nep_table} n
            WHERE n.scenario = :nep_scenario
            AND n.carrier = 'gas'
            AND n.chp = 'Nein'
        )
        SELECT
            (SELECT count(*) FROM ocgt_links) AS n_links,
            (
                SELECT sum(p_nom * efficiency)
                FROM ocgt_links
                WHERE bus0_carrier = 'CH4' AND bus1_carrier = 'AC'
            ) AS link_capacity,
            (SELECT count(*) FROM nep WHERE capacity > 0) AS n_nep,
            (SELECT sum(capacity) FROM nep WHERE capacity > 0)
                AS nep_capacity
        """

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {
            "scenario": self.scenario,
            "nep_scenario": self.params["nep_scenario"],
            "capacity_column": self.params["capacity_column"],
        }

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        n_links = int(row["n_links"])
        if n_links == 0:
            return self._no_links()

        n_nep = int(row["n_nep"])
        if n_nep == 0:
            return self._skip(
                f"No non-CHP gas plants with {self.params['capacity_column']} "
                f"in {self.nep_table} for scenario '{self.scenario}'; check "
                f"skipped"
            )

        observed = float(row["link_capacity"] or 0)
        expected = float(row["nep_capacity"])
        deviation = (observed - expected) / expected
        rtol = float(self.params.get("rtol", 0.01))
        success = abs(deviation) <= rtol

        return self._result(
            success=success,
            observed=observed,
            expected=expected,
            severity=Severity.INFO if success else Severity.WARNING,
            message=(
                f"Scenario '{self.scenario}': OCGT capacity "
                f"{observed:.1f} MW ({n_links} links) vs {expected:.1f} MW of "
                f"{n_nep} non-CHP gas plants in the NEP list (deviation "
                f"{deviation * 100:+.1f} %, tolerance {rtol * 100:.0f} %)"
            ),
        )


class OcgtScenarioCoverage(DataFrameRule):
    """
    Check that the German OCGT links cover exactly the run's scenarios.

    A configured scenario without links means the dataset wrote nothing for
    it; a scenario that is not configured means links are left over from a
    run with a different ``--scenarios``. The configured scenarios are read
    when the rule runs, not when the DAG is built.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        bus_table: str = "grid.egon_etrago_bus",
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            Link table, "grid.egon_etrago_link".
        rule_id : str
            Unique identifier for this validation rule.
        bus_table : str
            eTraGo bus table, used to restrict the check to German buses.
        """
        super().__init__(
            rule_id=rule_id,
            table=table,
            bus_table=bus_table,
            **kwargs,
        )
        self.kind = "sanity"
        self.bus_table = bus_table

    def get_query(self, ctx):
        return f"""
        SELECT array_agg(DISTINCT l.scn_name) AS scenarios
        FROM {self.table} l
        JOIN {self.bus_table} b0
            ON b0.bus_id = l.bus0
            AND b0.scn_name = l.scn_name
        JOIN {self.bus_table} b1
            ON b1.bus_id = l.bus1
            AND b1.scn_name = l.scn_name
        WHERE l.carrier = 'OCGT'
        AND b0.country = 'DE'
        AND b1.country = 'DE'
        """

    def evaluate_df(self, df, ctx):
        observed = set(df["scenarios"].values[0] or [])
        expected = set(
            egon.data.config.settings()["egon-data"]["--scenarios"]
        )
        missing = sorted(expected - observed)
        extra = sorted(observed - expected)
        success = not missing and not extra

        if success:
            message = (
                f"OCGT links exist for exactly the configured scenarios: "
                f"{', '.join(sorted(observed))}"
            )
        else:
            message = (
                f"Configured scenarios without OCGT links: "
                f"{', '.join(missing) or 'none'}; scenarios with OCGT links "
                f"that are not configured: {', '.join(extra) or 'none'}"
            )
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=len(observed),
            expected=len(expected),
            message=message,
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )

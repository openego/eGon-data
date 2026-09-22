"""
Sanity check validation rules for heat demand.

Validates that heat demand timeseries match expected values from peta_heat.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity


class HeatDemandValidation(DataFrameRule):
    """
    Validate annual heat demand against peta_heat reference values.

    Compares the sum of the heat demand load timeseries written by
    :func:`hts_to_etrago
    <egon.data.datasets.heat_etrago.hts_etrago.hts_to_etrago>` against the
    demand in ``demand.egon_peta_heat``, to check that the heat demand is
    fully and correctly distributed to the eTraGo tables.

    The carriers that carry heat demand depend on the scenario, and this rule
    mirrors ``hts_to_etrago`` exactly::

        central_heat, rural_heat, rural_gas_boiler   # reGon/eGon scenarios
        central_heat, rural_heat                     # status* scenarios

    Omitting ``rural_gas_boiler`` understates the demand massively in the
    reGon scenarios -- it is the largest of the three (measured for
    reGon2037: 346.5 TWh of 430.3 TWh).

    Some ``central_heat`` timeseries are entirely NaN (measured: 16 of 2983
    for reGon2037, 14 of 2302 for status2024). A plain ``SUM`` over them
    yields NaN, which this rule previously reported as "no heat demand data
    found". NaN entries are therefore skipped in SQL and counted separately
    so they show up in the message instead of destroying the total.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str = "reGon2037",
        rtol: float = 0.02,
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            Target table (grid.egon_etrago_load)
        rule_id : str
            Unique identifier for this validation rule
        scenario : str
            Scenario name, e.g. "reGon2037" or "status2024"
        rtol : float
            Relative tolerance for deviation (default: 0.02 = 2%)
        """
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            rtol=rtol,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario
        self.rtol = rtol

    # Carriers that hold heat demand loads, mirroring
    # `heat_etrago.hts_etrago.hts_to_etrago`.
    CARRIERS = ("central_heat", "rural_heat", "rural_gas_boiler")
    CARRIERS_STATUS = ("central_heat", "rural_heat")

    def heat_carriers(self):
        """Heat demand load carriers for this scenario."""
        if "status" in self.scenario:
            return list(self.CARRIERS_STATUS)
        return list(self.CARRIERS)

    def get_query(self, ctx):
        """
        Query to compare heat demand output vs input.

        Sums the heat demand load timeseries on German buses and the demand
        from ``demand.egon_peta_heat``. NaN entries are skipped rather than
        propagated, and the number of affected timeseries is returned so a
        NaN-only series cannot silently look like missing data.
        """
        return """
        WITH heat_loads AS (
            SELECT
                b.p_set AS p_set
            FROM grid.egon_etrago_load a
            JOIN grid.egon_etrago_load_timeseries b
              ON (a.load_id = b.load_id AND b.scn_name = a.scn_name)
            JOIN grid.egon_etrago_bus c
              ON (a.bus = c.bus_id AND c.scn_name = a.scn_name)
            WHERE a.scn_name = :scenario
            AND c.country = 'DE'
            AND a.carrier = ANY(:carriers)
        ),
        output_demand AS (
            SELECT
                SUM((
                    SELECT SUM(p) FROM UNNEST(p_set) p
                    WHERE p::double precision::text <> 'NaN'
                )) / 1000000 AS demand_twh,
                count(*) AS n_timeseries,
                count(*) FILTER (WHERE EXISTS (
                    SELECT 1 FROM UNNEST(p_set) v
                    WHERE v::double precision::text = 'NaN'
                )) AS n_timeseries_with_nan
            FROM heat_loads
        ),
        input_demand AS (
            SELECT
                SUM(demand / 1000000) AS demand_twh
            FROM demand.egon_peta_heat
            WHERE scenario = :scenario
        )
        SELECT
            o.demand_twh AS output_demand_twh,
            o.n_timeseries,
            o.n_timeseries_with_nan,
            i.demand_twh AS input_demand_twh
        FROM output_demand o
        CROSS JOIN input_demand i
        """

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {"scenario": self.scenario, "carriers": self.heat_carriers()}

    def evaluate_df(self, df, ctx):
        """
        Evaluate heat demand comparison.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with output_demand_twh, n_timeseries,
            n_timeseries_with_nan and input_demand_twh columns
        ctx : dict
            Context information

        Returns
        -------
        RuleResult
            Validation result with success/failure status
        """
        carriers = ", ".join(self.heat_carriers())
        n_timeseries = int(df["n_timeseries"].values[0] or 0)
        n_nan = int(df["n_timeseries_with_nan"].values[0] or 0)

        if df.empty or df["input_demand_twh"].isna().all():
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=(
                    f"No heat demand found in demand.egon_peta_heat for "
                    f"{self.scenario}"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        if n_timeseries == 0 or df["output_demand_twh"].isna().all():
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=(
                    f"No heat demand load timeseries for {self.scenario} on "
                    f"German buses (carriers: {carriers}); "
                    f"{n_timeseries} timeseries found"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        output_twh = float(df["output_demand_twh"].values[0])
        input_twh = float(df["input_demand_twh"].values[0])

        deviation = abs(output_twh - input_twh) / input_twh
        deviation_pct = deviation * 100
        diff_twh = output_twh - input_twh
        success = deviation <= self.rtol

        nan_note = (
            f", {n_nan} of {n_timeseries} timeseries contain NaN entries "
            f"(skipped)"
            if n_nan
            else ""
        )
        detail = (
            f"{output_twh:.2f} TWh vs {input_twh:.2f} TWh expected "
            f"(diff: {diff_twh:+.2f} TWh, deviation: {deviation_pct:.2f}%, "
            f"tolerance: {self.rtol * 100:.2f}%, carriers: {carriers}"
            f"{nan_note})"
        )

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=output_twh,
            expected=input_twh,
            message=(
                f"Heat demand valid for {self.scenario}: {detail}"
                if success
                else f"Heat demand deviation too large for "
                f"{self.scenario}: {detail}"
            ),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )

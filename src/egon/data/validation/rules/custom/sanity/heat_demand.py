"""
Sanity check validation rules for heat demand.

Validates that heat demand timeseries match expected values from peta_heat.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity


class HeatDemandValidation(DataFrameRule):
    """
    Validate annual heat demand against peta_heat reference values.

    Compares the sum of rural_heat and central_heat load timeseries
    against the demand from egon_peta_heat table to ensure demand is
    correctly distributed.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str = "eGon2035",
        rtol: float = 0.02,
        **kwargs
    ):
        """
        Parameters
        ----------
        table : str
            Target table (grid.egon_etrago_load)
        rule_id : str
            Unique identifier for this validation rule
        scenario : str
            Scenario name ("eGon2035" or "eGon100RE")
        rtol : float
            Relative tolerance for deviation (default: 0.02 = 2%)
        """
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            rtol=rtol,
            **kwargs
        )
        self.kind = "sanity"
        self.scenario = scenario
        self.rtol = rtol

    def get_query(self, ctx):
        """
        Query to compare heat demand output vs input.

        Returns a query that:
        1. Sums rural_heat + central_heat timeseries from etrago_load
        2. Sums demand from egon_peta_heat
        3. Returns both values for comparison
        """
        return f"""
        WITH output_demand AS (
            SELECT
                SUM((SELECT SUM(p) FROM UNNEST(b.p_set) p)) / 1000000 as demand_twh
            FROM grid.egon_etrago_load a
            JOIN grid.egon_etrago_load_timeseries b ON (a.load_id = b.load_id)
            JOIN grid.egon_etrago_bus c ON (a.bus = c.bus_id)
            WHERE b.scn_name = '{self.scenario}'
            AND a.scn_name = '{self.scenario}'
            AND c.scn_name = '{self.scenario}'
            AND c.country = 'DE'
            AND a.carrier IN ('rural_heat', 'central_heat')
        ),
        input_demand AS (
            SELECT
                SUM(demand / 1000000) as demand_twh
            FROM demand.egon_peta_heat
            WHERE scenario = '{self.scenario}'
        )
        SELECT
            o.demand_twh as output_demand_twh,
            i.demand_twh as input_demand_twh
        FROM output_demand o
        CROSS JOIN input_demand i
        """

    def evaluate_df(self, df, ctx):
        """
        Evaluate heat demand comparison.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with output_demand_twh and input_demand_twh columns
        ctx : dict
            Context information

        Returns
        -------
        RuleResult
            Validation result with success/failure status
        """
        if df.empty or df["output_demand_twh"].isna().all() or df["input_demand_twh"].isna().all():
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"No heat demand data found for {self.scenario}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

        output_twh = float(df["output_demand_twh"].values[0])
        input_twh = float(df["input_demand_twh"].values[0])

        # Calculate deviation
        deviation = abs(output_twh - input_twh) / input_twh
        deviation_pct = deviation * 100
        diff_twh = output_twh - input_twh

        success = deviation <= self.rtol

        if success:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=output_twh,
                expected=input_twh,
                message=(
                    f"Heat demand valid for {self.scenario}: "
                    f"{output_twh:.2f} TWh vs {input_twh:.2f} TWh expected "
                    f"(deviation: {deviation_pct:.2f}%, tolerance: {self.rtol*100:.2f}%)"
                ),
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )
        else:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=output_twh,
                expected=input_twh,
                message=(
                    f"Heat demand deviation too large for {self.scenario}: "
                    f"{output_twh:.2f} TWh vs {input_twh:.2f} TWh expected "
                    f"(diff: {diff_twh:+.2f} TWh, deviation: {deviation_pct:.2f}%, "
                    f"tolerance: {self.rtol*100:.2f}%)"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

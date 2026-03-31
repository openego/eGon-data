"""
Sanity check validation rules for electricity capacity comparison.

Validates that distributed capacities in etrago tables match input capacities
from scenario_capacities table.
"""

from typing import List, Optional

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity


class ElectricityCapacityComparison(DataFrameRule):
    """
    Compare distributed capacity with input capacity for electricity.

    Compares the total capacity in etrago tables
    (grid.egon_etrago_generator, grid.egon_etrago_storage) against the
    input capacity from the scenario capacities table
    (supply.egon_scenario_capacities).

    This validation ensures that capacity distribution is correct and no
    capacity is lost or incorrectly added during the distribution process.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str = "eGon2035",
        carrier: str = "wind_onshore",
        component_type: str = "generator",
        output_carriers: Optional[List[str]] = None,
        rtol: float = 0.10,
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            Target table (grid.egon_etrago_generator or
            grid.egon_etrago_storage)
        rule_id : str
            Unique identifier for this validation rule
        scenario : str
            Scenario name ("eGon2035" or "eGon100RE")
        carrier : str
            Carrier type for the input table (supply.egon_scenario_capacities)
        component_type : str
            Type of component ("generator", "storage", or "link")
        output_carriers : List[str], optional
            List of carrier names in output table. If None, uses carrier.
            Useful for biomass which maps to multiple output carriers.
        rtol : float
            Relative tolerance for capacity deviation (default: 0.10 = 10%)
        """
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            carrier=carrier,
            component_type=component_type,
            output_carriers=output_carriers,
            rtol=rtol,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario
        self.carrier = carrier
        self.component_type = component_type
        self.output_carriers = output_carriers or [carrier]
        self.rtol = rtol

    def get_query(self, ctx):
        """
        Query to compare input and output capacities.

        Returns a query that:
        1. Sums output capacity from etrago table for German buses
        2. Sums input capacity from scenario_capacities table
        3. Returns both values for comparison
        """
        # Build carrier filter for output table
        if len(self.output_carriers) == 1:
            carrier_filter = f"carrier = '{self.output_carriers[0]}'"
        else:
            carriers_str = "', '".join(self.output_carriers)
            carrier_filter = f"carrier IN ('{carriers_str}')"

        # Build bus filter based on component type
        # Links have bus0 and bus1, generators/storage have bus
        if self.component_type == "link":
            bus_filter = f"""
            AND (bus0 IN (
                SELECT bus_id
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{self.scenario}'
                AND country = 'DE'
            ) OR bus1 IN (
                SELECT bus_id
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{self.scenario}'
                AND country = 'DE'
            ))
            """
        else:
            bus_filter = f"""
            AND bus IN (
                SELECT bus_id
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{self.scenario}'
                AND country = 'DE'
            )
            """

        return f"""
        WITH output_capacity AS (
            SELECT
                COALESCE(SUM(p_nom::numeric), 0) as output_capacity_mw
            FROM {self.table}
            WHERE scn_name = '{self.scenario}'
            AND {carrier_filter}
            {bus_filter}
        ),
        input_capacity AS (
            SELECT
                COALESCE(SUM(capacity::numeric), 0) as input_capacity_mw
            FROM supply.egon_scenario_capacities
            WHERE carrier = '{self.carrier}'
            AND scenario_name = '{self.scenario}'
        )
        SELECT
            o.output_capacity_mw,
            i.input_capacity_mw
        FROM output_capacity o
        CROSS JOIN input_capacity i
        """

    def evaluate_df(self, df, ctx):
        """
        Evaluate capacity comparison.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with output_capacity_mw and input_capacity_mw columns
        ctx : dict
            Context information

        Returns
        -------
        RuleResult
            Validation result with success/failure status
        """
        if df.empty:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=(
                    f"No data found for {self.carrier} capacity comparison"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        output_capacity = float(df["output_capacity_mw"].values[0])
        input_capacity = float(df["input_capacity_mw"].values[0])

        # Case 1: Both zero - OK, no capacity needed
        if output_capacity == 0 and input_capacity == 0:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=0.0,
                expected=0.0,
                message=(
                    f"No {self.carrier} {self.component_type} capacity needed "
                    f"for {self.scenario} (both input and output are zero)"
                ),
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        # Case 2: Input > 0 but output = 0 - ERROR
        if input_capacity > 0 and output_capacity == 0:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=0.0,
                expected=input_capacity,
                message=(
                    f"{self.carrier} {self.component_type} capacity was not "
                    f"distributed at all! Input: {input_capacity:.2f} MW, "
                    f"Output: 0 MW for {self.scenario}"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        # Case 3: Output > 0 but input = 0 - ERROR
        if output_capacity > 0 and input_capacity == 0:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=output_capacity,
                expected=0.0,
                message=(
                    f"{self.carrier} {self.component_type} capacity was "
                    f"distributed even though no input was provided! "
                    f"Output: {output_capacity:.2f} MW, Input: 0 MW for "
                    f"{self.scenario}"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        # Case 4: Both > 0 - Check deviation
        deviation = abs(output_capacity - input_capacity) / input_capacity
        error_pct = ((output_capacity - input_capacity) / input_capacity) * 100

        success = deviation <= self.rtol

        if success:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=output_capacity,
                expected=input_capacity,
                message=(
                    f"{self.carrier} {self.component_type} capacity valid for "
                    f"{self.scenario}: Output: {output_capacity:.2f} MW, "
                    f"Input: {input_capacity:.2f} MW, Deviation: "
                    f"{error_pct:+.2f}% (tolerance: ±{self.rtol*100:.2f}%)"
                ),
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )
        else:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=output_capacity,
                expected=input_capacity,
                message=(
                    f"{self.carrier} {self.component_type} capacity deviation "
                    f"too large for {self.scenario}: "
                    f"Output: {output_capacity:.2f} MW, "
                    f"Input: {input_capacity:.2f} MW, Deviation: "
                    f"{error_pct:+.2f}% (tolerance: ±{self.rtol*100:.2f}%)"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

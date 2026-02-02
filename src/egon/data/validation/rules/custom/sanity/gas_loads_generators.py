"""
Sanity check validation rules for gas loads and generators.

Validates gas demand and generation capacity against reference data.
"""

from pathlib import Path
from typing import Optional, Any

import pandas as pd
import ast
from egon_validation.rules.base import DataFrameRule, RuleResult, Severity


class GasLoadsCapacity(DataFrameRule):
    """
    Validate gas loads capacity against reference data.

    Compares the total annual load (in TWh) for gas loads in Germany
    from the database against reference data from opendata.ffe.
    This validates that industrial gas demand (CH4 and H2) matches
    expected values from external sources.
    """

    def __init__(self, table: str, rule_id: str, scenario: str = "eGon2035",
                 carrier: str = "CH4_for_industry", rtol: float = 0.10,
                 expected_load: Optional[Any] = None, **kwargs):
        """
        Parameters
        ----------
        table : str
            Target table (grid.egon_etrago_load)
        rule_id : str
            Unique identifier for this validation rule
        scenario : str
            Scenario name ("eGon2035" or "eGon100RE")
        carrier : str
            Load carrier type ("CH4_for_industry" or "H2_for_industry")
        rtol : float
            Relative tolerance for capacity deviation (default: 0.10 = 10%)
        expected_load : Optional[Any]
            Expected load in TWh. If None, calculates from reference data.
            Can be boundary-dependent via resolve_boundary_dependence().
        """
        super().__init__(rule_id=rule_id, table=table, scenario=scenario,
                         carrier=carrier, rtol=rtol, expected_load=expected_load,
                         **kwargs)
        self.kind = "sanity"
        self.scenario = scenario
        self.carrier = carrier

    def get_query(self, ctx):
        """
        Query to get total annual load for gas loads in Germany.

        Returns a query that sums the annual load from timeseries data
        for the specified carrier in Germany, converting to TWh.
        """
        return f"""
        SELECT (SUM(
            (SELECT SUM(p)
            FROM UNNEST(b.p_set) p))/1000000)::numeric as load_twh
        FROM grid.egon_etrago_load a
        JOIN grid.egon_etrago_load_timeseries b
        ON (a.load_id = b.load_id)
        JOIN grid.egon_etrago_bus c
        ON (a.bus=c.bus_id)
        WHERE b.scn_name = '{self.scenario}'
        AND a.scn_name = '{self.scenario}'
        AND c.scn_name = '{self.scenario}'
        AND c.country = 'DE'
        AND a.carrier = '{self.carrier}'
        """

    def _get_reference_capacity(self):
        """
        Calculate reference load capacity from opendata.ffe data.

        Returns
        -------
        float
            Expected total annual load in TWh
        """
        try:
            path = Path(".") / "datasets" / "gas_data" / "demand"

            # Read region correlation file
            corr_file = path / "region_corr.json"
            df_corr = pd.read_json(corr_file)
            df_corr = df_corr.loc[:, ["id_region", "name_short"]]
            df_corr.set_index("id_region", inplace=True)

            # Read demand data for carrier
            input_gas_demand = pd.read_json(
                path / (self.carrier + f"_{self.scenario}.json")
            )
            input_gas_demand = input_gas_demand.loc[:, ["id_region", "value"]]
            input_gas_demand.set_index("id_region", inplace=True)

            # Join with correlation and filter for Germany
            input_gas_demand = pd.concat(
                [input_gas_demand, df_corr], axis=1, join="inner"
            )
            input_gas_demand["NUTS0"] = (input_gas_demand["name_short"].str)[0:2]
            input_gas_demand = input_gas_demand[
                input_gas_demand["NUTS0"].str.match("DE")
            ]

            # Sum and convert to TWh
            total_demand = sum(input_gas_demand.value.to_list()) / 1000000

            return float(total_demand)

        except Exception as e:
            raise ValueError(f"Error reading reference load data: {str(e)}")

    def evaluate_df(self, df, ctx):
        """
        Evaluate gas loads capacity against reference data.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with load_twh column
        ctx : dict
            Context information

        Returns
        -------
        RuleResult
            Validation result with success/failure status
        """
        if df.empty or df["load_twh"].isna().all():
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"No {self.carrier} loads found for scenario {self.scenario}",
                severity=Severity.WARNING,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

        observed_load = float(df["load_twh"].values[0])

        # Get expected load - use param if provided, otherwise calculate
        expected_load = self.params.get("expected_load")
        if expected_load is None:
            try:
                expected_load = self._get_reference_capacity()
            except Exception as e:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    message=str(e),
                    severity=Severity.ERROR,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__
                )

        # Calculate relative deviation
        rtol = self.params.get("rtol", 0.10)
        deviation = abs(observed_load - expected_load) / expected_load

        success = deviation <= rtol
        deviation_pct = deviation * 100

        if success:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=observed_load,
                expected=expected_load,
                message=(
                    f"{self.carrier} load valid for {self.scenario}: "
                    f"{observed_load:.2f} TWh (deviation: {deviation_pct:.2f}%, "
                    f"tolerance: {rtol*100:.2f}%)"
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
                observed=observed_load,
                expected=expected_load,
                message=(
                    f"{self.carrier} load deviation too large for {self.scenario}: "
                    f"{observed_load:.2f} vs {expected_load:.2f} TWh expected "
                    f"(deviation: {deviation_pct:.2f}%, tolerance: {rtol*100:.2f}%)"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )


class GasGeneratorsCapacity(DataFrameRule):
    """
    Validate gas generators capacity against reference data.

    Compares the total nominal power (p_nom) of CH4 generators in Germany
    from the database against reference data from SciGRID_gas productions
    and the Biogaspartner Einspeiseatlas.
    """

    def __init__(self, table: str, rule_id: str, scenario: str = "eGon2035",
                 carrier: str = "CH4", rtol: float = 0.10,
                 expected_capacity: Optional[Any] = None, **kwargs):
        """
        Parameters
        ----------
        table : str
            Target table (grid.egon_etrago_generator)
        rule_id : str
            Unique identifier for this validation rule
        scenario : str
            Scenario name ("eGon2035" or "eGon100RE")
        carrier : str
            Generator carrier type (default: "CH4")
        rtol : float
            Relative tolerance for capacity deviation (default: 0.10 = 10%)
        expected_capacity : Optional[Any]
            Expected capacity in MW. If None, calculates from reference data.
            Can be boundary-dependent via resolve_boundary_dependence().
        """
        super().__init__(rule_id=rule_id, table=table, scenario=scenario,
                         carrier=carrier, rtol=rtol, expected_capacity=expected_capacity,
                         **kwargs)
        self.kind = "sanity"
        self.scenario = scenario
        self.carrier = carrier

    def get_query(self, ctx):
        """
        Query to get total generator capacity in Germany.

        Returns a query that sums the p_nom of all gas generators
        in Germany for the specified carrier.
        """
        return f"""
        SELECT SUM(p_nom::numeric) as p_nom_germany
        FROM grid.egon_etrago_generator
        WHERE scn_name = '{self.scenario}'
        AND carrier = '{self.carrier}'
        AND bus IN (
            SELECT bus_id
            FROM grid.egon_etrago_bus
            WHERE scn_name = '{self.scenario}'
            AND country = 'DE'
            AND carrier = '{self.carrier}'
        )
        """

    def _get_reference_capacity(self):
        """
        Calculate reference generation capacity from SciGRID_gas + biogas data.

        Returns
        -------
        float
            Expected total generation capacity in MW
        """
        try:
            # Read SciGRID_gas natural gas productions
            target_file = (
                Path(".")
                / "datasets"
                / "gas_data"
                / "data"
                / "IGGIELGN_Productions.csv"
            )

            ng_generators = pd.read_csv(
                target_file,
                delimiter=";",
                decimal=".",
                usecols=["country_code", "param"],
            )

            ng_generators = ng_generators[
                ng_generators["country_code"].str.match("DE")
            ]

            # Sum natural gas production capacity
            p_ng = 0
            for index, row in ng_generators.iterrows():
                param = ast.literal_eval(row["param"])
                p_ng = p_ng + param["max_supply_M_m3_per_d"]

            conversion_factor = 437.5  # MCM/day to MWh/h
            p_ng = p_ng * conversion_factor

            # Read biogas production data
            basename = "Biogaspartner_Einspeiseatlas_Deutschland_2021.xlsx"
            target_file = (
                Path(".") / "data_bundle_egon_data" / "gas_data" / basename
            )

            conversion_factor_b = 0.01083  # m^3/h to MWh/h
            p_biogas = (
                pd.read_excel(
                    target_file,
                    usecols=["Einspeisung Biomethan [(N*m^3)/h)]"],
                )["Einspeisung Biomethan [(N*m^3)/h)]"].sum()
                * conversion_factor_b
            )

            total_generation = p_ng + p_biogas

            return float(total_generation)

        except Exception as e:
            raise ValueError(f"Error reading reference generation data: {str(e)}")

    def evaluate_df(self, df, ctx):
        """
        Evaluate gas generators capacity against reference data.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with p_nom_germany column
        ctx : dict
            Context information

        Returns
        -------
        RuleResult
            Validation result with success/failure status
        """
        if df.empty or df["p_nom_germany"].isna().all():
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"No {self.carrier} generators found for scenario {self.scenario}",
                severity=Severity.WARNING,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

        observed_capacity = float(df["p_nom_germany"].values[0])

        # Get expected capacity - use param if provided, otherwise calculate
        expected_capacity = self.params.get("expected_capacity")
        if expected_capacity is None:
            try:
                expected_capacity = self._get_reference_capacity()
            except Exception as e:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    message=str(e),
                    severity=Severity.ERROR,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__
                )

        # Calculate relative deviation
        rtol = self.params.get("rtol", 0.10)
        deviation = abs(observed_capacity - expected_capacity) / expected_capacity

        success = deviation <= rtol
        deviation_pct = deviation * 100

        if success:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=observed_capacity,
                expected=expected_capacity,
                message=(
                    f"{self.carrier} generator capacity valid for {self.scenario}: "
                    f"{observed_capacity:.2f} MW (deviation: {deviation_pct:.2f}%, "
                    f"tolerance: {rtol*100:.2f}%)"
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
                observed=observed_capacity,
                expected=expected_capacity,
                message=(
                    f"{self.carrier} generator capacity deviation too large for {self.scenario}: "
                    f"{observed_capacity:.2f} vs {expected_capacity:.2f} MW expected "
                    f"(deviation: {deviation_pct:.2f}%, tolerance: {rtol*100:.2f}%)"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

"""
Sanity check validation rules for gas storage components.

Validates CH4 and H2 storage capacities against expected values from
grid capacities and external data sources.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity

from egon.data import config
from egon.data.datasets.hydrogen_etrago.storage import (
    calculate_and_map_saltcavern_storage_potential
)


class CH4StoresCapacity(DataFrameRule):
    """
    Validate CH4 store capacity in Germany.

    Compares the sum of CH4 store capacities in the database against the
    expected capacity calculated from:
    - CH4 grid capacity allocation
    - Total CH4 store capacity in Germany (source: GIE)

    The check allows for small deviations between observed and expected values.
    """

    def __init__(self, table: str, rule_id: str, scenario: str = "eGon2035",
                 rtol: float = 0.02, **kwargs):
        """
        Parameters
        ----------
        table : str
            Target table (grid.egon_etrago_store)
        rule_id : str
            Unique identifier for this validation rule
        scenario : str
            Scenario name ("eGon2035" or "eGon100RE")
        rtol : float
            Relative tolerance for capacity deviation (default: 0.02 = 2%)
        """
        super().__init__(rule_id=rule_id, table=table, scenario=scenario,
                         rtol=rtol, **kwargs)
        self.kind = "sanity"
        self.scenario = scenario

    def get_query(self, ctx):
        """
        Query to get total CH4 store capacity in Germany.

        Returns a query that sums all CH4 store capacities for German buses
        in the specified scenario.
        """
        return f"""
        SELECT SUM(e_nom::numeric) as e_nom_germany
        FROM grid.egon_etrago_store
        WHERE scn_name = '{self.scenario}'
        AND carrier = 'CH4'
        AND bus IN (
            SELECT bus_id
            FROM grid.egon_etrago_bus
            WHERE scn_name = '{self.scenario}'
            AND country = 'DE'
            AND carrier = 'CH4'
        )
        """

    def evaluate_df(self, df, ctx):
        """
        Evaluate CH4 store capacity against expected values.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with e_nom_germany column
        ctx : dict
            Context information

        Returns
        -------
        RuleResult
            Validation result with success/failure status
        """
        if df.empty or df["e_nom_germany"].isna().all():
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"No CH4 store data found for scenario {self.scenario}",
                severity=Severity.WARNING,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

        observed_capacity = float(df["e_nom_germany"].values[0])

        # Calculate expected capacity based on scenario
        if self.scenario == "eGon2035":
            grid_cap = 130000  # MWh
        elif self.scenario == "eGon100RE":
            # Get retrofitted share from config
            from egon.data.datasets.scenario_parameters import get_sector_parameters
            retrofitted_share = get_sector_parameters("gas", "eGon100RE")[
                "retrofitted_CH4pipeline-to-H2pipeline_share"
            ]
            grid_cap = 13000 * (1 - retrofitted_share)  # MWh
        else:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"Unknown scenario: {self.scenario}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

        # GIE capacity: https://www.gie.eu/transparency/databases/storage-database/
        stores_cap_germany = 266424202  # MWh

        expected_capacity = stores_cap_germany + grid_cap

        # Calculate relative deviation
        rtol = self.params.get("rtol", 0.02)
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
                    f"CH4 stores capacity valid for {self.scenario}: "
                    f"deviation {deviation_pct:.2f}% (tolerance: {rtol*100:.2f}%)"
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
                    f"CH4 stores capacity deviation too large for {self.scenario}: "
                    f"{deviation_pct:.2f}% (tolerance: {rtol*100:.2f}%)"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )


class H2SaltcavernStoresCapacity(DataFrameRule):
    """
    Validate H2 saltcavern store potential capacity in Germany.

    Compares the sum of H2 saltcavern potential storage capacities (e_nom_max)
    in the database against the expected capacity calculated from:
    - Area fractions around substations in federal states
    - Estimated total hydrogen storage potential per federal state (InSpEE-DS)

    The check allows for small deviations between observed and expected values.
    """

    def __init__(self, table: str, rule_id: str, scenario: str = "eGon2035",
                 rtol: float = 0.02, **kwargs):
        """
        Parameters
        ----------
        table : str
            Target table (grid.egon_etrago_store)
        rule_id : str
            Unique identifier for this validation rule
        scenario : str
            Scenario name ("eGon2035" or "eGon100RE")
        rtol : float
            Relative tolerance for capacity deviation (default: 0.02 = 2%)
        """
        super().__init__(rule_id=rule_id, table=table, scenario=scenario,
                         rtol=rtol, **kwargs)
        self.kind = "sanity"
        self.scenario = scenario

    def get_query(self, ctx):
        """
        Query to get total H2 saltcavern potential storage capacity in Germany.

        Returns a query that sums all H2_underground store e_nom_max capacities
        for German H2_saltcavern buses in the specified scenario.
        """
        return f"""
        SELECT SUM(e_nom_max::numeric) as e_nom_max_germany
        FROM grid.egon_etrago_store
        WHERE scn_name = '{self.scenario}'
        AND carrier = 'H2_underground'
        AND bus IN (
            SELECT bus_id
            FROM grid.egon_etrago_bus
            WHERE scn_name = '{self.scenario}'
            AND country = 'DE'
            AND carrier = 'H2_saltcavern'
        )
        """

    def evaluate_df(self, df, ctx):
        """
        Evaluate H2 saltcavern storage capacity against expected values.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with e_nom_max_germany column
        ctx : dict
            Context information

        Returns
        -------
        RuleResult
            Validation result with success/failure status
        """
        if df.empty or df["e_nom_max_germany"].isna().all():
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"No H2 saltcavern store data found for scenario {self.scenario}",
                severity=Severity.WARNING,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

        observed_capacity = float(df["e_nom_max_germany"].values[0])

        # Calculate expected capacity from saltcavern potential
        try:
            storage_potentials = calculate_and_map_saltcavern_storage_potential()
            storage_potentials["storage_potential"] = (
                storage_potentials["area_fraction"] * storage_potentials["potential"]
            )
            expected_capacity = sum(storage_potentials["storage_potential"].to_list())
        except Exception as e:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"Error calculating expected H2 saltcavern capacity: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

        # Calculate relative deviation
        rtol = self.params.get("rtol", 0.02)
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
                    f"H2 saltcavern stores capacity valid for {self.scenario}: "
                    f"deviation {deviation_pct:.2f}% (tolerance: {rtol*100:.2f}%)"
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
                    f"H2 saltcavern stores capacity deviation too large for {self.scenario}: "
                    f"{deviation_pct:.2f}% (tolerance: {rtol*100:.2f}%)"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

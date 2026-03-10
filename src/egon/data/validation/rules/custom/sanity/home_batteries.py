"""
Sanity check validation rules for home batteries

Validates that home battery capacities are correctly aggregated
from building-level to bus-level in the storages table.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity
import pandas as pd

from egon.data import config
from egon.data.validation.rules.custom.sanity.utils import get_cbat_pbat_ratio


class HomeBatteriesAggregation(DataFrameRule):
    """
    Validate home battery capacity aggregation from buildings to buses.

    This rule checks that the sum of home battery capacities allocated to
    buildings matches the aggregated capacity per bus in the storage table.

    The check compares:
    1. p_nom (power rating in MW) per bus
    2. capacity (energy capacity in MWh) per bus

    Both values are rounded to 6 decimal places for comparison.
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str = "eGon2035", **kwargs
    ):
        super().__init__(
            rule_id=rule_id, table=table, scenario=scenario, **kwargs
        )
        self.kind = "sanity"
        self.scenario = scenario

    def get_query(self, ctx):
        """
        Query to compare storage and building-level home battery data.

        Returns a joined query that compares aggregated building-level data
        with the storage table data per bus.
        """
        # Get table names from config
        sources = config.datasets()["home_batteries"]["sources"]
        targets = config.datasets()["home_batteries"]["targets"]

        # Get cbat_pbat_ratio for capacity calculation
        cbat_pbat_ratio = get_cbat_pbat_ratio(self.scenario)

        storage_schema = sources["storage"]["schema"]
        storage_table = sources["storage"]["table"]
        hb_schema = targets["home_batteries"]["schema"]
        hb_table = targets["home_batteries"]["table"]

        return f"""
        WITH storage_data AS (
            SELECT
                bus_id,
                el_capacity as storage_p_nom,
                el_capacity * {cbat_pbat_ratio} as storage_capacity
            FROM {storage_schema}.{storage_table}
            WHERE carrier = 'home_battery'
            AND scenario = '{self.scenario}'
        ),
        building_data AS (
            SELECT
                bus_id,
                SUM(p_nom) as building_p_nom,
                SUM(capacity) as building_capacity
            FROM {hb_schema}.{hb_table}
            WHERE scenario = '{self.scenario}'
            GROUP BY bus_id
        )
        SELECT
            COALESCE(s.bus_id, b.bus_id) as bus_id,
            ROUND(s.storage_p_nom::numeric, 6) as storage_p_nom,
            ROUND(s.storage_capacity::numeric, 6) as storage_capacity,
            ROUND(b.building_p_nom::numeric, 6) as building_p_nom,
            ROUND(b.building_capacity::numeric, 6) as building_capacity
        FROM storage_data s
        FULL OUTER JOIN building_data b ON s.bus_id = b.bus_id
        ORDER BY bus_id
        """

    def evaluate_df(self, df, ctx):
        """
        Evaluate the comparison between storage and building data.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with storage and building data per bus
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
                    f"No home battery data found for scenario {self.scenario}"
                ),
                severity=Severity.WARNING,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        # Check for buses that exist in only one source
        missing_in_storage = df[df["storage_p_nom"].isna()]
        missing_in_buildings = df[df["building_p_nom"].isna()]

        if not missing_in_storage.empty or not missing_in_buildings.empty:
            violations = []
            if not missing_in_storage.empty:
                bus_list = missing_in_storage["bus_id"].tolist()[:5]
                violations.append(
                    f"{len(missing_in_storage)} bus(es) in buildings "
                    f"but not in storage: {bus_list}"
                )
            if not missing_in_buildings.empty:
                bus_list = missing_in_buildings["bus_id"].tolist()[:5]
                violations.append(
                    f"{len(missing_in_buildings)} bus(es) in storage "
                    f"but not in buildings: {bus_list}"
                )

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=len(missing_in_storage) + len(missing_in_buildings),
                expected=0,
                message=f"Bus mismatch: {'; '.join(violations)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        # Check if p_nom values match
        p_nom_mismatch = df[df["storage_p_nom"] != df["building_p_nom"]]

        # Check if capacity values match
        cap_mismatch = df[df["storage_capacity"] != df["building_capacity"]]

        # Combine mismatches
        mismatches = pd.concat([p_nom_mismatch, cap_mismatch]).drop_duplicates(
            subset=["bus_id"]
        )

        if not mismatches.empty:
            # Calculate maximum differences
            p_nom_diff = df["storage_p_nom"] - df["building_p_nom"]
            cap_diff = df["storage_capacity"] - df["building_capacity"]
            max_p_nom_diff = p_nom_diff.abs().max()
            max_capacity_diff = cap_diff.abs().max()

            # Get all violations
            cols = [
                "bus_id",
                "storage_p_nom",
                "building_p_nom",
                "storage_capacity",
                "building_capacity",
            ]
            all_violations = mismatches[cols].to_dict(orient="records")

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=float(max(max_p_nom_diff, max_capacity_diff)),
                expected=0.0,
                message=(
                    f"Home battery aggregation mismatch for "
                    f"{len(mismatches)} bus(es): "
                    f"max p_nom diff={max_p_nom_diff:.6f}, "
                    f"max capacity diff={max_capacity_diff:.6f}. "
                    f"violations: {all_violations}"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        # All checks passed
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=True,
            observed=0.0,
            expected=0.0,
            message=(
                f"Home battery capacities correctly aggregated for all "
                f"{len(df)} buses in scenario {self.scenario}"
            ),
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )

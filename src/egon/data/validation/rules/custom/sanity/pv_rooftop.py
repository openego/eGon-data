"""
Sanity check validation rules for PV rooftop buildings.

Validates PV rooftop capacity allocation to buildings against
expected scenario values.
"""

from math import isclose
from pathlib import Path

import pandas as pd
from egon_validation.rules.base import Rule, RuleResult, Severity

from egon.data import config, db
from egon.data.datasets.power_plants.pv_rooftop_buildings import (
    PV_CAP_PER_SQ_M,
    ROOF_FACTOR,
    load_building_data,
    scenario_data,
)


class PvRooftopBuildingsValidation(Rule):
    """
    Validate PV rooftop capacity allocation to buildings.

    This rule checks:
    1. All PV rooftop installations are assigned to valid buildings
    2. Total capacity matches expected scenario values

    For eGon2035: Compares with scenario_data() capacity sum
    For eGon100RE: Compares with scenario_capacities table
    (with boundary adjustment for Schleswig-Holstein)
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str = "eGon2035",
        rtol: float = 0.01, **kwargs
    ):
        super().__init__(rule_id=rule_id, table=table, scenario=scenario,
                         rtol=rtol, **kwargs)
        self.kind = "sanity"
        self.scenario = scenario

    def _get_pv_roof_data(self):
        """Load PV rooftop building data from database."""
        sql = """
        SELECT *
        FROM supply.egon_power_plants_pv_roof_building
        """
        return db.select_dataframe(sql, index_col="index")

    def _check_building_assignment(self, pv_roof_df, valid_buildings_gdf):
        """
        Check that all PV rooftop installations have valid building assignments.

        Returns
        -------
        tuple
            (success, missing_count, message)
        """
        merge_df = pv_roof_df.merge(
            valid_buildings_gdf[["building_area"]],
            how="left",
            left_on="building_id",
            right_index=True,
        )

        missing_count = len(merge_df.loc[merge_df.building_area.isna()])

        if missing_count > 0:
            return False, missing_count, merge_df, (
                f"{missing_count} PV rooftop installations have no valid "
                f"building assignment"
            )

        return True, 0, merge_df, "All PV installations assigned to valid buildings"

    def _get_expected_capacity_egon2035(self):
        """Get expected capacity for eGon2035 scenario."""
        return scenario_data(scenario="eGon2035").capacity.sum()

    def _get_expected_capacity_egon100re(self):
        """
        Get expected capacity for eGon100RE scenario.

        Includes boundary adjustment for Schleswig-Holstein.
        """
        sources = config.datasets()["solar_rooftop"]["sources"]

        target = db.select_dataframe(
            f"""
            SELECT capacity
            FROM {sources['scenario_capacities']['schema']}.
            {sources['scenario_capacities']['table']}
            WHERE carrier = 'solar_rooftop'
            AND scenario_name = 'eGon100RE'
            """
        ).capacity[0]

        dataset = config.settings()["egon-data"]["--dataset-boundary"]

        if dataset == "Schleswig-Holstein":
            sources = config.datasets()["scenario_input"]["sources"]

            path = Path(
                f"./data_bundle_egon_data/nep2035_version2021/"
                f"{sources['eGon2035']['capacities']}"
            ).resolve()

            total_2035 = (
                pd.read_excel(
                    path,
                    sheet_name="1.Entwurf_NEP2035_V2021",
                    index_col="Unnamed: 0",
                ).at["PV (Aufdach)", "Summe"]
                * 1000
            )
            sh_2035 = scenario_data(scenario="eGon2035").capacity.sum()

            share = sh_2035 / total_2035

            target *= share

        return target

    def evaluate(self, ctx):
        """
        Evaluate PV rooftop building assignments and capacities.

        Parameters
        ----------
        ctx : dict
            Context information

        Returns
        -------
        RuleResult
            Validation result with success/failure status
        """
        try:
            # Load data
            pv_roof_df = self._get_pv_roof_data()

            if pv_roof_df.empty:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    message="No PV rooftop building data found",
                    severity=Severity.WARNING,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__
                )

            valid_buildings_gdf = load_building_data()

            valid_buildings_gdf = valid_buildings_gdf.assign(
                bus_id=valid_buildings_gdf.bus_id.astype(int),
                overlay_id=valid_buildings_gdf.overlay_id.astype(int),
                max_cap=valid_buildings_gdf.building_area.multiply(
                    ROOF_FACTOR * PV_CAP_PER_SQ_M
                ),
            )

            # Check building assignment
            bldg_ok, missing, merge_df, bldg_msg = self._check_building_assignment(
                pv_roof_df, valid_buildings_gdf
            )

            if not bldg_ok:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    observed=missing,
                    expected=0,
                    message=bldg_msg,
                    severity=Severity.ERROR,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__
                )

            # Get observed capacity for this scenario
            scenario_df = merge_df.loc[merge_df.scenario == self.scenario]
            observed_capacity = scenario_df.capacity.sum()

            # Get expected capacity based on scenario
            if self.scenario == "eGon2035":
                expected_capacity = self._get_expected_capacity_egon2035()
            elif self.scenario == "eGon100RE":
                expected_capacity = self._get_expected_capacity_egon100re()
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

            # Compare capacities
            rtol = self.params.get("rtol", 0.01)
            capacity_ok = isclose(
                expected_capacity,
                observed_capacity,
                rel_tol=rtol
            )

            if capacity_ok:
                deviation_pct = abs(observed_capacity - expected_capacity) / expected_capacity * 100
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    observed=observed_capacity,
                    expected=expected_capacity,
                    message=(
                        f"PV rooftop capacity valid for {self.scenario}: "
                        f"{observed_capacity:.2f} kW (deviation: {deviation_pct:.2f}%, "
                        f"tolerance: {rtol*100:.2f}%)"
                    ),
                    severity=Severity.INFO,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__
                )
            else:
                deviation_pct = abs(observed_capacity - expected_capacity) / expected_capacity * 100
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    observed=observed_capacity,
                    expected=expected_capacity,
                    message=(
                        f"PV rooftop capacity mismatch for {self.scenario}: "
                        f"{observed_capacity:.2f} vs {expected_capacity:.2f} kW "
                        f"(deviation: {deviation_pct:.2f}%, tolerance: {rtol*100:.2f}%)"
                    ),
                    severity=Severity.ERROR,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__
                )

        except Exception as e:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"Error validating PV rooftop buildings: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )
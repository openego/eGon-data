"""
Sanity check validation rules for electrical load sector breakdown.

Validates that electrical loads are correctly disaggregated into sectors
(residential, commercial, industrial) and that each sector matches expected values.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity
from egon.data import config, db
import pandas as pd


class ElectricalLoadSectorBreakdown(DataFrameRule):
    """
    Validate electrical load breakdown by sector (residential, commercial, industrial).

    This rule checks that the electrical load for each sector matches expected values:
    - Residential: 90.4 TWh (from household_curves)
    - Commercial: 146.7 TWh (from cts_curves)
    - Industrial: 382.9 TWh (from osm_curves + sites_curves)
    - Total: 620.0 TWh (from etrago AC loads)

    Matches the original electrical_load_100RE() function from sanity_checks.py.
    """

    def __init__(self, table: str, rule_id: str, scenario: str = "eGon100RE",
                 rtol: float = 0.01, **kwargs):
        """
        Parameters
        ----------
        table : str
            Target table (grid.egon_etrago_load)
        rule_id : str
            Unique identifier for this validation rule
        scenario : str
            Scenario name (default: "eGon100RE")
        rtol : float
            Relative tolerance for load deviation (default: 0.01 = 1%)
        """
        super().__init__(rule_id=rule_id, table=table, scenario=scenario,
                         rtol=rtol, **kwargs)
        self.kind = "sanity"
        self.scenario = scenario
        self.rtol = rtol

    def get_query(self, ctx):
        """
        Query to get total AC electrical load for Germany.

        Returns total load in TWh from etrago tables.
        """
        return f"""
        SELECT SUM((SELECT SUM(p) FROM UNNEST(b.p_set) p))/1000000::numeric as load_twh
        FROM grid.egon_etrago_load a
        JOIN grid.egon_etrago_load_timeseries b
            ON (a.load_id = b.load_id)
        JOIN grid.egon_etrago_bus c
            ON (a.bus = c.bus_id)
        WHERE a.scn_name = '{self.scenario}'
            AND b.scn_name = '{self.scenario}'
            AND c.scn_name = '{self.scenario}'
            AND a.carrier = 'AC'
            AND c.country = 'DE'
        """

    def _get_sector_loads(self):
        """
        Get electrical loads by sector from source tables.

        Returns
        -------
        dict
            Dictionary with sector loads in TWh:
            - residential: TWh from household_curves
            - commercial: TWh from cts_curves
            - industrial: TWh from osm_curves + sites_curves
        """
        sources = config.datasets()["etrago_electricity"]["sources"]

        # Commercial load from CTS curves
        cts_curves = db.select_dataframe(
            f"""SELECT bus_id AS bus, p_set FROM
                {sources['cts_curves']['schema']}.
                {sources['cts_curves']['table']}
                WHERE scn_name = '{self.scenario}'""",
            warning=False
        )
        commercial_twh = (
            cts_curves.apply(lambda x: sum(x["p_set"]), axis=1).sum() / 1000000
        )

        # Industrial load from OSM landuse areas
        ind_curves_osm = db.select_dataframe(
            f"""SELECT bus, p_set FROM
                {sources['osm_curves']['schema']}.
                {sources['osm_curves']['table']}
                WHERE scn_name = '{self.scenario}'""",
            warning=False
        )
        industrial_osm_twh = (
            ind_curves_osm.apply(lambda x: sum(x["p_set"]), axis=1).sum() / 1000000
        )

        # Industrial load from industrial sites
        ind_curves_sites = db.select_dataframe(
            f"""SELECT bus, p_set FROM
                {sources['sites_curves']['schema']}.
                {sources['sites_curves']['table']}
                WHERE scn_name = '{self.scenario}'""",
            warning=False
        )
        industrial_sites_twh = (
            ind_curves_sites.apply(lambda x: sum(x["p_set"]), axis=1).sum() / 1000000
        )

        # Total industrial
        industrial_twh = industrial_osm_twh + industrial_sites_twh

        # Residential load from household curves
        hh_curves = db.select_dataframe(
            f"""SELECT bus_id AS bus, p_set FROM
                {sources['household_curves']['schema']}.
                {sources['household_curves']['table']}
                WHERE scn_name = '{self.scenario}'""",
            warning=False
        )
        residential_twh = (
            hh_curves.apply(lambda x: sum(x["p_set"]), axis=1).sum() / 1000000
        )

        return {
            "residential": residential_twh,
            "commercial": commercial_twh,
            "industrial": industrial_twh
        }

    def evaluate_df(self, df, ctx):
        """
        Evaluate electrical load sector breakdown.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with total load_twh column
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
                message=f"No electrical load data found for scenario {self.scenario}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

        # Get total AC load
        total_load_twh = float(df["load_twh"].values[0])

        # Get sector loads
        try:
            sector_loads = self._get_sector_loads()
        except Exception as e:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"Error reading sector load data: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

        # Expected values (from original sanity_checks.py lines 2689-2694)
        # References:
        # https://github.com/openego/powerd-data/blob/56b8215928a8dc4fe953d266c563ce0ed98e93f9/src/egon/data/datasets/demandregio/__init__.py#L480
        # https://github.com/openego/powerd-data/blob/56b8215928a8dc4fe953d266c563ce0ed98e93f9/src/egon/data/datasets/demandregio/__init__.py#L775
        expected_values = {
            "residential": 90.4,
            "commercial": 146.7,
            "industrial": 382.9,
            "total": 620.0
        }

        # Build load summary dataframe
        load_summary = pd.DataFrame({
            "sector": ["residential", "commercial", "industrial", "total"],
            "expected": [
                expected_values["residential"],
                expected_values["commercial"],
                expected_values["industrial"],
                expected_values["total"]
            ],
            "observed": [
                sector_loads["residential"],
                sector_loads["commercial"],
                sector_loads["industrial"],
                total_load_twh
            ]
        })

        load_summary["diff"] = load_summary["observed"] - load_summary["expected"]
        load_summary["diff_pct"] = (
            load_summary["diff"] / load_summary["observed"] * 100
        )

        # Check if all deviations are within tolerance (< 1% as in original)
        violations = load_summary[load_summary["diff_pct"].abs() >= (self.rtol * 100)]

        if not violations.empty:
            # Format violation details
            violation_details = []
            for _, row in violations.iterrows():
                violation_details.append(
                    f"{row['sector']}: {row['observed']:.2f} TWh "
                    f"(expected {row['expected']:.2f} TWh, "
                    f"deviation {row['diff_pct']:+.2f}%)"
                )

            max_deviation = load_summary["diff_pct"].abs().max()

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=float(max_deviation),
                expected=self.rtol * 100,
                message=(
                    f"Electrical load sector breakdown deviations exceed tolerance for {self.scenario}: "
                    f"{'; '.join(violation_details)}"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

        # All sectors within tolerance
        sector_summary = "; ".join([
            f"{row['sector']}: {row['observed']:.2f} TWh "
            f"(expected {row['expected']:.2f} TWh, "
            f"deviation {row['diff_pct']:+.2f}%)"
            for _, row in load_summary.iterrows()
        ])

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=True,
            observed=0.0,
            expected=0.0,
            message=(
                f"Electrical load sector breakdown valid for {self.scenario}: {sector_summary}"
            ),
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__
        )

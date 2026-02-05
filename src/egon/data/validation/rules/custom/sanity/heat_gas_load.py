"""
Sanity check validation rules for heat and gas loads comparison with PyPSA-Eur.

Validates that etrago load data matches PyPSA-Eur network data for
heat and gas carriers.

Note: The original function heat_gas_load_egon100RE() only printed a
comparison table without pass/fail logic. These rules add threshold-based
validation per carrier.
"""

import numpy as np
import pandas as pd
from egon_validation.rules.base import Rule, RuleResult, Severity

from egon.data import db
from egon.data.datasets.pypsaeur import read_network


class HeatGasLoadPypsaEurComparison(Rule):
    """
    Compare heat and gas loads with PyPSA-Eur network data.

    Validates that the total load per carrier in etrago matches
    the corresponding load in PyPSA-Eur within a tolerance.
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str = "eGon100RE",
        carrier: str = None, rtol: float = 0.10, **kwargs
    ):
        """
        Parameters
        ----------
        table : str
            Target table (grid.egon_etrago_load)
        rule_id : str
            Unique identifier for this validation rule
        scenario : str
            Scenario name (default: "eGon100RE")
        carrier : str
            Carrier to validate (e.g., "rural_heat", "central_heat",
            "H2_for_industry", "CH4_for_industry"). If None, validates all.
        rtol : float
            Relative tolerance for deviation (default: 0.10 = 10%)
        """
        super().__init__(rule_id=rule_id, table=table, scenario=scenario,
                         carrier=carrier, rtol=rtol, **kwargs)
        self.kind = "sanity"
        self.scenario = scenario
        self.carrier = carrier

    def _get_load_carrier_dict(self):
        """Dictionary for matching pypsa_eur carrier with egon-data carriers."""
        return {
            "DE0 0 land transport EV": "land transport EV",
            "DE0 0 rural heat": "rural_heat",
            "DE0 0 urban central heat": "central_heat",
            "DE0 0 urban decentral heat": "rural_heat",
            "rural heat": "rural_heat",
            "H2 for industry": "H2_for_industry",
            "gas for industry": "CH4_for_industry",
            "urban central heat": "central_heat",
            "urban decentral heat": "rural_heat",
            "land transport EV": "land transport EV",
        }

    def _get_etrago_loads(self):
        """Get loads from etrago, excluding NaN central_heat timeseries."""
        # Filter out NaN values in central_heat timeseries
        NaN_load_ids = db.select_dataframe(
            """
            SELECT load_id from grid.egon_etrago_load_timeseries
            WHERE load_id IN (Select load_id
                FROM grid.egon_etrago_load
                WHERE carrier = 'central_heat') AND (SELECT
                bool_or(value::double precision::text = 'NaN')
            FROM unnest(p_set) AS value
            )
           """
        )
        nan_load_list = tuple(NaN_load_ids["load_id"].tolist())

        if nan_load_list:
            nan_load_str = ",".join(map(str, nan_load_list))
            nan_filter = f"AND l.load_id NOT IN ({nan_load_str})"
        else:
            nan_filter = ""

        loads_etrago_timeseries = db.select_dataframe(
            f"""
                SELECT
                    l.carrier,
                    SUM(
                        (SELECT SUM(p)
                        FROM UNNEST(t.p_set) p)
                    )  AS total_p_set_timeseries
                FROM
                    grid.egon_etrago_load l
                LEFT JOIN
                    grid.egon_etrago_load_timeseries t ON l.load_id = t.load_id
                WHERE
                    l.scn_name = '{self.scenario}'
                    AND l.carrier != 'AC'
                    AND l.bus IN (
                        SELECT bus_id
                        FROM grid.egon_etrago_bus
                        WHERE scn_name = '{self.scenario}'
                        AND country = 'DE'
                    )
                    {nan_filter}
                GROUP BY
                    l.carrier
            """
        )
        return loads_etrago_timeseries

    def _get_pypsa_eur_loads(self):
        """Get loads from PyPSA-Eur network."""
        load_carrier_dict = self._get_load_carrier_dict()
        n = read_network()

        # Aggregate loads with values in timeseries dataframe
        df_load_timeseries = n.loads_t.p_set
        filtered_columns = [
            col
            for col in df_load_timeseries.columns
            if col.startswith("DE") and "electricity" not in col
        ]
        german_loads_timeseries = df_load_timeseries[filtered_columns]
        if "DE0 0" in german_loads_timeseries.columns:
            german_loads_timeseries = german_loads_timeseries.drop(columns=["DE0 0"])
        german_loads_timeseries = german_loads_timeseries.mul(
            n.snapshot_weightings.generators, axis=0
        ).sum()
        german_loads_timeseries = german_loads_timeseries.rename(
            index=load_carrier_dict
        )

        # Sum loads with fixed p_set in loads dataframe
        german_load_static_p_set = n.loads[
            n.loads.index.str.startswith("DE")
            & ~n.loads.carrier.str.contains("electricity")
        ]
        german_load_static_p_set = (
            german_load_static_p_set.groupby("carrier").p_set.sum() * 8760
        )
        german_load_static_p_set = german_load_static_p_set.rename(
            index=load_carrier_dict
        )

        # Add H2 from Fischer-Tropsch and methanolisation links
        if "H2_for_industry" in german_load_static_p_set.index:
            ft_links = n.links.loc[
                n.links.index.str.contains("DE0 0 Fischer-Tropsch")
            ].index
            if len(ft_links) > 0 and ft_links[0] in n.links_t.p0.columns:
                german_load_static_p_set["H2_for_industry"] = (
                    german_load_static_p_set["H2_for_industry"]
                    + n.links_t.p0[ft_links]
                    .mul(n.snapshot_weightings.generators, axis=0)
                    .sum()
                    .sum()
                )

            meth_links = n.links.loc[
                n.links.index.str.contains("DE0 0 methanolisation")
            ].index
            if len(meth_links) > 0 and meth_links[0] in n.links_t.p0.columns:
                german_load_static_p_set["H2_for_industry"] = (
                    german_load_static_p_set["H2_for_industry"]
                    + n.links_t.p0[meth_links]
                    .mul(n.snapshot_weightings.generators, axis=0)
                    .sum()
                    .sum()
                )

        # Combine p_set and timeseries dataframes
        german_loads_timeseries_df = german_loads_timeseries.to_frame()
        german_loads_timeseries_df["carrier"] = german_loads_timeseries_df.index
        german_loads_timeseries_df.set_index("carrier", inplace=True)

        german_load_static_p_set_df = german_load_static_p_set.to_frame()
        german_load_static_p_set_df = german_load_static_p_set_df.groupby(
            "carrier", as_index=True
        ).sum()
        german_loads_timeseries_df = german_loads_timeseries_df.groupby(
            "carrier", as_index=True
        ).sum()
        combined = pd.merge(
            german_load_static_p_set_df,
            german_loads_timeseries_df,
            on="carrier",
            how="left",
        )

        combined["p_set"] = np.where(
            combined["p_set"] == 0, combined[0], combined["p_set"]
        )
        if 0 in combined.columns:
            combined = combined.drop(columns=[0])

        return combined

    def evaluate(self, engine, ctx):
        """Evaluate heat and gas loads against PyPSA-Eur data."""
        try:
            # Get data from both sources
            loads_etrago = self._get_etrago_loads()
            loads_pypsa = self._get_pypsa_eur_loads()

            if loads_etrago.empty:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    message=f"No etrago load data found for scenario {self.scenario}",
                    severity=Severity.WARNING,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__
                )

            # Build comparison dataframe
            carriers_loads = set(
                loads_pypsa.index.union(loads_etrago["carrier"])
            )

            loads_capacities = pd.DataFrame(
                index=list(carriers_loads), columns=["pypsa_eur", self.scenario]
            )
            loads_capacities[self.scenario] = loads_etrago.groupby(
                "carrier"
            ).total_p_set_timeseries.sum()
            loads_capacities["pypsa_eur"] = loads_pypsa["p_set"]
            loads_capacities["diff_pct"] = (
                (loads_capacities[self.scenario] - loads_capacities["pypsa_eur"])
                / loads_capacities["pypsa_eur"].replace(0, np.nan)
            ) * 100

            rtol = self.params.get("rtol", 0.10)

            # If specific carrier requested, validate only that carrier
            if self.carrier:
                if self.carrier not in loads_capacities.index:
                    return RuleResult(
                        rule_id=self.rule_id,
                        task=self.task,
                        table=self.table,
                        kind=self.kind,
                        success=False,
                        message=f"Carrier {self.carrier} not found in data",
                        severity=Severity.WARNING,
                        schema=self.schema,
                        table_name=self.table_name,
                        rule_class=self.__class__.__name__
                    )

                row = loads_capacities.loc[self.carrier]
                observed = row[self.scenario]
                expected = row["pypsa_eur"]
                diff_pct = row["diff_pct"]

                if pd.isna(expected) or expected == 0:
                    return RuleResult(
                        rule_id=self.rule_id,
                        task=self.task,
                        table=self.table,
                        kind=self.kind,
                        success=True,
                        message=(
                            f"No PyPSA-Eur reference for {self.carrier}, "
                            f"etrago value: {observed}"
                        ),
                        severity=Severity.INFO,
                        schema=self.schema,
                        table_name=self.table_name,
                        rule_class=self.__class__.__name__
                    )

                success = abs(diff_pct / 100) <= rtol

                if success:
                    return RuleResult(
                        rule_id=self.rule_id,
                        task=self.task,
                        table=self.table,
                        kind=self.kind,
                        success=True,
                        observed=float(observed) if not pd.isna(observed) else None,
                        expected=float(expected),
                        message=(
                            f"{self.carrier} load for {self.scenario}: "
                            f"deviation {diff_pct:.2f}% (tolerance: {rtol*100:.2f}%)"
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
                        observed=float(observed) if not pd.isna(observed) else None,
                        expected=float(expected),
                        message=(
                            f"{self.carrier} load deviation for {self.scenario}: "
                            f"{diff_pct:.2f}% (tolerance: {rtol*100:.2f}%)"
                        ),
                        severity=Severity.ERROR,
                        schema=self.schema,
                        table_name=self.table_name,
                        rule_class=self.__class__.__name__
                    )

            # Validate all carriers
            errors = []
            for carrier in loads_capacities.index:
                row = loads_capacities.loc[carrier]
                expected = row["pypsa_eur"]
                diff_pct = row["diff_pct"]

                if pd.isna(expected) or expected == 0:
                    continue

                if pd.isna(diff_pct):
                    errors.append(f"{carrier}: no etrago data")
                elif abs(diff_pct / 100) > rtol:
                    errors.append(f"{carrier}: {diff_pct:.2f}%")

            if errors:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    message=(
                        f"Heat/gas load deviations for {self.scenario} "
                        f"(tolerance: {rtol*100:.2f}%): {'; '.join(errors)}"
                    ),
                    severity=Severity.ERROR,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__
                )

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                message=(
                    f"All heat/gas loads for {self.scenario} within tolerance "
                    f"({rtol*100:.2f}%) of PyPSA-Eur"
                ),
                severity=Severity.INFO,
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
                message=f"Error comparing heat/gas loads: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )
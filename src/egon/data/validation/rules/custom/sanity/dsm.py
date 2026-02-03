"""
Sanity check validation rules for DSM (Demand Side Management).

Validates that DSM link and store timeseries match the aggregated
individual DSM data from CTS and industry tables.
"""

import numpy as np
import pandas as pd
from egon_validation.rules.base import Rule, RuleResult, Severity

from egon.data import config, db


class DSMTimeseries(Rule):
    """
    Validate DSM timeseries aggregation from individual components.

    This rule checks that:
    1. DSM link timeseries (p_min_pu, p_max_pu * p_nom) match the sum of
       individual p_min/p_max from DSM tables
    2. DSM store timeseries (e_min_pu, e_max_pu * e_nom) match the sum of
       individual e_min/e_max from DSM tables

    The individual DSM data comes from 4 tables:
    - cts_loadcurves_dsm
    - ind_osm_loadcurves_individual_dsm
    - demandregio_ind_sites_dsm
    - ind_sites_loadcurves_individual

    Uses np.allclose() with atol=1e-01 for power comparisons due to
    time series clipping at zero.
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str = "eGon2035", **kwargs
    ):
        super().__init__(rule_id=rule_id, table=table, scenario=scenario, **kwargs)
        self.kind = "sanity"
        self.scenario = scenario

    def _df_from_series(self, s: pd.Series):
        """Convert series of arrays to DataFrame."""
        return pd.DataFrame.from_dict(dict(zip(s.index, s.values)))

    def _get_individual_dsm_data(self):
        """
        Load individual DSM data from all DSM tables.

        Returns
        -------
        pd.DataFrame
            Combined DSM data with columns: bus, p_min, p_max, e_min, e_max
        """
        targets = config.datasets()["DSM_CTS_industry"]["targets"]

        tables = [
            "cts_loadcurves_dsm",
            "ind_osm_loadcurves_individual_dsm",
            "demandregio_ind_sites_dsm",
            "ind_sites_loadcurves_individual",
        ]

        df_list = []

        for table in tables:
            target = targets[table]
            sql = f"""
            SELECT bus, p_min, p_max, e_max, e_min
            FROM {target["schema"]}.{target["table"]}
            WHERE scn_name = '{self.scenario}'
            ORDER BY bus
            """
            df_list.append(db.select_dataframe(sql))

        return pd.concat(df_list, ignore_index=True)

    def _validate_link_timeseries(self, individual_ts_df, groups):
        """
        Validate DSM link timeseries (p_min, p_max).

        Returns
        -------
        tuple
            (success, message)
        """
        # Get link metadata
        sql = f"""
        SELECT link_id, bus0 as bus, p_nom FROM grid.egon_etrago_link
        WHERE carrier = 'dsm'
        AND scn_name = '{self.scenario}'
        ORDER BY link_id
        """
        meta_df = db.select_dataframe(sql, index_col="link_id")

        if meta_df.empty:
            return False, f"No DSM links found for scenario {self.scenario}"

        link_ids = str(meta_df.index.tolist())[1:-1]

        # Get link timeseries
        sql = f"""
        SELECT link_id, p_min_pu, p_max_pu
        FROM grid.egon_etrago_link_timeseries
        WHERE scn_name = '{self.scenario}'
        AND link_id IN ({link_ids})
        ORDER BY link_id
        """
        ts_df = db.select_dataframe(sql, index_col="link_id")

        if ts_df.empty:
            return False, f"No DSM link timeseries found for scenario {self.scenario}"

        # Calculate actual power values
        p_max_df = self._df_from_series(ts_df.p_max_pu).mul(meta_df.p_nom)
        p_min_df = self._df_from_series(ts_df.p_min_pu).mul(meta_df.p_nom)

        p_max_df.columns = meta_df.bus.tolist()
        p_min_df.columns = meta_df.bus.tolist()

        # Aggregate individual p_max by bus
        individual_p_max_df = self._df_from_series(individual_ts_df.p_max)
        individual_p_max_df = pd.DataFrame(
            [
                individual_p_max_df[idxs].sum(axis=1)
                for idxs in groups.values()
            ],
            index=groups.keys(),
        ).T

        # Aggregate individual p_min by bus
        individual_p_min_df = self._df_from_series(individual_ts_df.p_min)
        individual_p_min_df = pd.DataFrame(
            [
                individual_p_min_df[idxs].sum(axis=1)
                for idxs in groups.values()
            ],
            index=groups.keys(),
        ).T

        # Compare with tolerance (atol=1e-01 due to clipping at zero)
        atol = 1e-01
        p_max_ok = np.allclose(p_max_df, individual_p_max_df, atol=atol)
        p_min_ok = np.allclose(p_min_df, individual_p_min_df, atol=atol)

        if p_max_ok and p_min_ok:
            return True, "DSM link timeseries (p_min, p_max) match individual data"

        errors = []
        if not p_max_ok:
            max_diff = np.abs(p_max_df.values - individual_p_max_df.values).max()
            errors.append(f"p_max mismatch (max diff: {max_diff:.4f})")
        if not p_min_ok:
            max_diff = np.abs(p_min_df.values - individual_p_min_df.values).max()
            errors.append(f"p_min mismatch (max diff: {max_diff:.4f})")

        return False, "; ".join(errors)

    def _validate_store_timeseries(self, individual_ts_df, groups):
        """
        Validate DSM store timeseries (e_min, e_max).

        Returns
        -------
        tuple
            (success, message)
        """
        # Get store metadata
        sql = f"""
        SELECT store_id, bus, e_nom FROM grid.egon_etrago_store
        WHERE carrier = 'dsm'
        AND scn_name = '{self.scenario}'
        ORDER BY store_id
        """
        meta_df = db.select_dataframe(sql, index_col="store_id")

        if meta_df.empty:
            return False, f"No DSM stores found for scenario {self.scenario}"

        store_ids = str(meta_df.index.tolist())[1:-1]

        # Get store timeseries
        sql = f"""
        SELECT store_id, e_min_pu, e_max_pu
        FROM grid.egon_etrago_store_timeseries
        WHERE scn_name = '{self.scenario}'
        AND store_id IN ({store_ids})
        ORDER BY store_id
        """
        ts_df = db.select_dataframe(sql, index_col="store_id")

        if ts_df.empty:
            return False, f"No DSM store timeseries found for scenario {self.scenario}"

        # Calculate actual energy values
        e_max_df = self._df_from_series(ts_df.e_max_pu).mul(meta_df.e_nom)
        e_min_df = self._df_from_series(ts_df.e_min_pu).mul(meta_df.e_nom)

        e_max_df.columns = meta_df.bus.tolist()
        e_min_df.columns = meta_df.bus.tolist()

        # Aggregate individual e_max by bus
        individual_e_max_df = self._df_from_series(individual_ts_df.e_max)
        individual_e_max_df = pd.DataFrame(
            [
                individual_e_max_df[idxs].sum(axis=1)
                for idxs in groups.values()
            ],
            index=groups.keys(),
        ).T

        # Aggregate individual e_min by bus
        individual_e_min_df = self._df_from_series(individual_ts_df.e_min)
        individual_e_min_df = pd.DataFrame(
            [
                individual_e_min_df[idxs].sum(axis=1)
                for idxs in groups.values()
            ],
            index=groups.keys(),
        ).T

        # Compare with default tolerance
        e_max_ok = np.allclose(e_max_df, individual_e_max_df)
        e_min_ok = np.allclose(e_min_df, individual_e_min_df)

        if e_max_ok and e_min_ok:
            return True, "DSM store timeseries (e_min, e_max) match individual data"

        errors = []
        if not e_max_ok:
            max_diff = np.abs(e_max_df.values - individual_e_max_df.values).max()
            errors.append(f"e_max mismatch (max diff: {max_diff:.4f})")
        if not e_min_ok:
            max_diff = np.abs(e_min_df.values - individual_e_min_df.values).max()
            errors.append(f"e_min mismatch (max diff: {max_diff:.4f})")

        return False, "; ".join(errors)

    def evaluate(self, ctx):
        """
        Evaluate DSM timeseries against individual component data.

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
            # Load individual DSM data
            individual_ts_df = self._get_individual_dsm_data()

            if individual_ts_df.empty:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    message=f"No individual DSM data found for scenario {self.scenario}",
                    severity=Severity.WARNING,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__
                )

            # Group by bus
            groups = individual_ts_df[["bus"]].reset_index().groupby("bus").groups

            # Validate link timeseries (p_min, p_max)
            link_ok, link_msg = self._validate_link_timeseries(
                individual_ts_df, groups
            )

            # Validate store timeseries (e_min, e_max)
            store_ok, store_msg = self._validate_store_timeseries(
                individual_ts_df, groups
            )

            if link_ok and store_ok:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    message=(
                        f"DSM timeseries valid for {self.scenario}: "
                        f"link and store data match individual components"
                    ),
                    severity=Severity.INFO,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__
                )

            # Collect all errors
            errors = []
            if not link_ok:
                errors.append(f"Link: {link_msg}")
            if not store_ok:
                errors.append(f"Store: {store_msg}")

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"DSM timeseries mismatch for {self.scenario}: {'; '.join(errors)}",
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
                message=f"Error validating DSM timeseries: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )
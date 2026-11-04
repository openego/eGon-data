"""Sanity check validation rules for demand-side management."""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity


class DSMValues(DataFrameRule):
    """Validate DSM time-series values for a specific scenario."""

    def __init__(self, table: str, rule_id: str, scenario: str, **kwargs):
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario

    def get_params(self, ctx):
        del ctx
        return {"scenario": self.scenario}

    def get_query(self, ctx):
        del ctx
        return f"""
        SELECT
            count(*) AS n_rows,

            count(*) FILTER (
                WHERE p_set IS NULL
                   OR p_max IS NULL
                   OR p_min IS NULL
                   OR e_max IS NULL
                   OR e_min IS NULL
            ) AS n_null_arrays,

            count(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM unnest(p_set, p_max, p_min, e_max, e_min)
                    AS v(ps, pmax, pmin, emax, emin)
                    WHERE ps IS NULL OR ps = 'NaN'
                       OR pmax IS NULL OR pmax = 'NaN'
                       OR pmin IS NULL OR pmin = 'NaN'
                       OR emax IS NULL OR emax = 'NaN'
                       OR emin IS NULL OR emin = 'NaN'
                )
            ) AS n_invalid_values,

            count(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM unnest(p_set) AS v
                    WHERE v < 0
                )
            ) AS n_negative_p_set,

            count(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM unnest(p_max) AS v
                    WHERE v < 0
                )
            ) AS n_negative_p_max,

            count(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM unnest(p_min) AS v
                    WHERE v > 0
                )
            ) AS n_positive_p_min,

            count(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM unnest(e_max) AS v
                    WHERE v < 0
                )
            ) AS n_negative_e_max,

            count(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM unnest(e_min) AS v
                    WHERE v > 0
                )
            ) AS n_positive_e_min

        FROM {self.table}
        WHERE scn_name = :scenario
        """

    def evaluate_df(self, df, ctx):
        del ctx
        row = df.iloc[0]

        n_rows = int(row.n_rows)
        n_null_arrays = int(row.n_null_arrays)
        n_invalid_values = int(row.n_invalid_values)
        n_negative_p_set = int(row.n_negative_p_set)
        n_negative_p_max = int(row.n_negative_p_max)
        n_positive_p_min = int(row.n_positive_p_min)
        n_negative_e_max = int(row.n_negative_e_max)
        n_positive_e_min = int(row.n_positive_e_min)

        n_bad = (
            n_null_arrays
            + n_invalid_values
            + n_negative_p_set
            + n_negative_p_max
            + n_positive_p_min
            + n_negative_e_max
            + n_positive_e_min
        )

        success = n_rows > 0 and n_bad == 0

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=n_bad,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': {n_rows} DSM rows, "
                f"{n_null_arrays} rows with NULL arrays, "
                f"{n_invalid_values} rows with NULL/NaN values, "
                f"{n_negative_p_set} rows with negative p_set, "
                f"{n_negative_p_max} rows with negative p_max, "
                f"{n_positive_p_min} rows with positive p_min, "
                f"{n_negative_e_max} rows with negative e_max, "
                f"{n_positive_e_min} rows with positive e_min"
            ),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )


class DSMEtragoValues(DataFrameRule):
    """Validate DSM eTraGo components and their time series."""

    def __init__(self, rule_id: str, scenario: str, **kwargs):
        super().__init__(
            rule_id=rule_id,
            table="grid.egon_etrago_link",
            scenario=scenario,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario

    def get_params(self, ctx):
        del ctx
        return {"scenario": self.scenario}

    def get_query(self, ctx):
        del ctx
        return """
        WITH link_checks AS (
            SELECT
                count(*) AS n_links,

                count(*) FILTER (
                    WHERE b0.bus_id IS NULL
                       OR b1.bus_id IS NULL
                       OR b1.carrier IS DISTINCT FROM 'dsm'
                       OR l.p_nom IS NULL
                       OR l.p_nom <= 0
                ) AS n_invalid_links,

                count(*) FILTER (
                    WHERE t.link_id IS NULL
                       OR t.p_min_pu IS NULL
                       OR t.p_max_pu IS NULL
                       OR cardinality(t.p_min_pu) != 8760
                       OR cardinality(t.p_max_pu) != 8760
                ) AS n_invalid_link_lengths,

                count(*) FILTER (
                    WHERE EXISTS (
                        SELECT 1
                        FROM unnest(t.p_min_pu) AS v
                        WHERE v IS NULL
                           OR v = 'NaN'
                           OR v < -1
                           OR v > 0
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM unnest(t.p_max_pu) AS v
                        WHERE v IS NULL
                           OR v = 'NaN'
                           OR v < 0
                           OR v > 1
                    )
                ) AS n_invalid_link_values

            FROM grid.egon_etrago_link l

            LEFT JOIN grid.egon_etrago_bus b0
                ON b0.scn_name = l.scn_name
                AND b0.bus_id = l.bus0

            LEFT JOIN grid.egon_etrago_bus b1
                ON b1.scn_name = l.scn_name
                AND b1.bus_id = l.bus1

            LEFT JOIN grid.egon_etrago_link_timeseries t
                ON t.scn_name = l.scn_name
                AND t.link_id = l.link_id

            WHERE l.scn_name = :scenario
              AND l.carrier = 'dsm'
        ),

        store_checks AS (
            SELECT
                count(*) AS n_stores,

                count(*) FILTER (
                    WHERE b.bus_id IS NULL
                       OR b.carrier IS DISTINCT FROM 'dsm'
                       OR s.e_nom IS NULL
                       OR s.e_nom <= 0
                ) AS n_invalid_stores,

                count(*) FILTER (
                    WHERE t.store_id IS NULL
                       OR t.e_min_pu IS NULL
                       OR t.e_max_pu IS NULL
                       OR cardinality(t.e_min_pu) != 8760
                       OR cardinality(t.e_max_pu) != 8760
                ) AS n_invalid_store_lengths,

                count(*) FILTER (
                    WHERE EXISTS (
                        SELECT 1
                        FROM unnest(t.e_min_pu) AS v
                        WHERE v IS NULL
                           OR v = 'NaN'
                           OR v < -1
                           OR v > 0
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM unnest(t.e_max_pu) AS v
                        WHERE v IS NULL
                           OR v = 'NaN'
                           OR v < 0
                           OR v > 1
                    )
                ) AS n_invalid_store_values

            FROM grid.egon_etrago_store s

            LEFT JOIN grid.egon_etrago_bus b
                ON b.scn_name = s.scn_name
                AND b.bus_id = s.bus

            LEFT JOIN grid.egon_etrago_store_timeseries t
                ON t.scn_name = s.scn_name
                AND t.store_id = s.store_id

            WHERE s.scn_name = :scenario
              AND s.carrier = 'dsm'
        ),

        bus_checks AS (
            SELECT
                count(*) AS n_buses,

                count(*) FILTER (
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM grid.egon_etrago_link l
                        WHERE l.scn_name = b.scn_name
                          AND l.bus1 = b.bus_id
                          AND l.carrier = 'dsm'
                    )
                    OR NOT EXISTS (
                        SELECT 1
                        FROM grid.egon_etrago_store s
                        WHERE s.scn_name = b.scn_name
                          AND s.bus = b.bus_id
                          AND s.carrier = 'dsm'
                    )
                ) AS n_unconnected_buses

            FROM grid.egon_etrago_bus b

            WHERE b.scn_name = :scenario
              AND b.carrier = 'dsm'
        )

        SELECT *
        FROM link_checks
        CROSS JOIN store_checks
        CROSS JOIN bus_checks
        """

    def evaluate_df(self, df, ctx):
        del ctx
        row = df.iloc[0]

        n_links = int(row.n_links)
        n_stores = int(row.n_stores)
        n_buses = int(row.n_buses)

        n_invalid_links = int(row.n_invalid_links)
        n_invalid_link_lengths = int(row.n_invalid_link_lengths)
        n_invalid_link_values = int(row.n_invalid_link_values)

        n_invalid_stores = int(row.n_invalid_stores)
        n_invalid_store_lengths = int(row.n_invalid_store_lengths)
        n_invalid_store_values = int(row.n_invalid_store_values)

        n_unconnected_buses = int(row.n_unconnected_buses)

        n_bad = (
            n_invalid_links
            + n_invalid_link_lengths
            + n_invalid_link_values
            + n_invalid_stores
            + n_invalid_store_lengths
            + n_invalid_store_values
            + n_unconnected_buses
        )

        success = (
            n_links > 0
            and n_stores > 0
            and n_buses > 0
            and n_bad == 0
        )

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=n_bad,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': "
                f"{n_buses} DSM buses, "
                f"{n_links} DSM links, "
                f"{n_stores} DSM stores, "
                f"{n_invalid_links} invalid links, "
                f"{n_invalid_link_lengths} invalid link arrays, "
                f"{n_invalid_link_values} invalid link values, "
                f"{n_invalid_stores} invalid stores, "
                f"{n_invalid_store_lengths} invalid store arrays, "
                f"{n_invalid_store_values} invalid store values, "
                f"{n_unconnected_buses} unconnected DSM buses"
            ),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )


class DSMSourceCoverage(DataFrameRule):
    """Check DSM source rows against eTraGo DSM links."""

    def __init__(self, rule_id: str, scenario: str, **kwargs):
        super().__init__(
            rule_id=rule_id,
            table="grid.egon_etrago_link",
            scenario=scenario,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario

    def get_params(self, ctx):
        del ctx
        return {"scenario": self.scenario}

    def get_query(self, ctx):
        del ctx
        return """
        WITH sources AS (
            SELECT scn_name, bus
            FROM demand.egon_etrago_electricity_cts_dsm_timeseries
            WHERE scn_name = :scenario

            UNION ALL

            SELECT scn_name, bus
            FROM demand.egon_osm_ind_load_curves_individual_dsm_timeseries
            WHERE scn_name = :scenario

            UNION ALL

            SELECT scn_name, bus
            FROM demand.egon_demandregio_sites_ind_electricity_dsm_timeseries
            WHERE scn_name = :scenario

            UNION ALL

            SELECT scn_name, bus
            FROM demand.egon_sites_ind_load_curves_individual_dsm_timeseries
            WHERE scn_name = :scenario
        )

        SELECT
            count(*) AS n_sources,

            count(*) FILTER (
                WHERE s.bus IS NULL
                   OR NOT EXISTS (
                       SELECT 1
                       FROM grid.egon_etrago_link l
                       JOIN grid.egon_etrago_bus b
                           ON b.scn_name = l.scn_name
                           AND b.bus_id = l.bus1
                           AND b.carrier = 'dsm'
                       WHERE l.scn_name = s.scn_name
                         AND l.bus0 = s.bus
                         AND l.carrier = 'dsm'
                   )
            ) AS n_missing_sources

        FROM sources s
        """

    def evaluate_df(self, df, ctx):
        del ctx
        row = df.iloc[0]

        n_sources = int(row.n_sources)
        n_missing = int(row.n_missing_sources)

        success = n_sources > 0 and n_missing == 0

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=n_missing,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': "
                f"{n_sources} DSM source rows, "
                f"{n_missing} without corresponding DSM links"
            ),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )
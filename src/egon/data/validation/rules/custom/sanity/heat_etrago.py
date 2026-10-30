"""
Sanity check validation rules for the heat sector in eTraGo.

Validates what :class:`~egon.data.datasets.heat_etrago.HeatEtrago` writes
into the eTraGo tables: the central and rural heat buses, the heat supply
technologies taken from ``supply.egon_district_heating`` and
``supply.egon_individual_heating`` (heat pumps, resistive heaters, gas
boilers, solar thermal, geothermal) and the extendable heat stores.

All rules only look at heat buses in Germany and at components attached
to them.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity
import pandas as pd

import egon.data.config

#: Heat supply technologies as ``(eTraGo carrier, eTraGo component, source
#: table key, source carrier)``. The eTraGo capacity is ``p_nom``, except for
#: gas boilers, whose ``p_nom`` is the gas input, so there it is
#: ``p_nom * efficiency``.
TECHNOLOGIES = [
    ("central_heat_pump", "link", "district", "heat_pump"),
    ("central_resistive_heater", "link", "district", "resistive_heater"),
    ("central_gas_boiler", "link", "district", "gas_boiler"),
    ("solar_thermal_collector", "generator", "district",
     "solar_thermal_collector"),
    ("geo_thermal", "generator", "district", "geo_thermal"),
    ("rural_heat_pump", "link", "individual", "heat_pump"),
    ("rural_resistive_heater", "link", "individual", "resistive_heater"),
    ("rural_gas_boiler", "link", "individual", "gas_boiler"),
    ("rural_solar_thermal", "generator", "individual", "solar_thermal"),
]

#: Heat buses and the stores attached to each of them.
HEAT_BUS_CARRIERS = ("central_heat", "rural_heat")


class _HeatRule(DataFrameRule):
    """
    Common base for the per-scenario rules of the heat sector.

    Subclasses return exactly one row, because the framework reports an
    empty query result as a failure.

    A scenario that is absent from the tables is **not** an error: the rule
    reports success with severity INFO, because which scenarios a run
    produces depends on ``--scenarios``.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        bus_table: str = "grid.egon_etrago_bus",
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            eTraGo table the rule is reported for.
        rule_id : str
            Unique identifier for this validation rule.
        scenario : str
            Scenario to check, e.g. "status2024".
        bus_table : str
            eTraGo bus table, used to restrict the check to German buses.
        """
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            bus_table=bus_table,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario
        self.bus_table = bus_table

    def _heat_buses_cte(self):
        """CTE ``heat_buses``: German central and rural heat buses."""
        carriers = ", ".join(f"'{c}'" for c in HEAT_BUS_CARRIERS)
        return f"""
        heat_buses AS (
            SELECT bus_id, carrier
            FROM {self.bus_table}
            WHERE scn_name = :scenario
            AND country = 'DE'
            AND carrier IN ({carriers})
        )
        """

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {"scenario": self.scenario}

    def _result(self, success, message, observed=None, expected=None,
                severity=None):
        """A result with the common fields filled in."""
        if severity is None:
            severity = Severity.INFO if success else Severity.ERROR
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=observed,
            expected=expected,
            message=message,
            severity=severity,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )

    def _skip(self, message):
        """A non-failure result for cases that cannot be checked."""
        return self._result(success=True, message=message)

    def _no_heat_buses(self):
        return self._skip(
            f"Scenario '{self.scenario}' has no German heat buses in "
            f"{self.bus_table}; check skipped"
        )


class HeatSupplyCapacity(_HeatRule):
    """
    Compare the heat supply capacity per technology with its source table.

    For each technology the eTraGo capacity on German heat buses must add
    up to the capacity in ``supply.egon_district_heating`` (central) or
    ``supply.egon_individual_heating`` (rural). Gas boiler links carry the
    gas input as ``p_nom``, so for them ``p_nom * efficiency`` is compared.
    A technology with no capacity on either side is not reported.

    ``severity`` sets how a deviation is reported: gas boilers use WARNING,
    because some of them are dropped while matching the supply areas to
    their heat bus, which is a known issue of the dataset.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        technologies,
        link_table: str = "grid.egon_etrago_link",
        generator_table: str = "grid.egon_etrago_generator",
        district_table: str = "supply.egon_district_heating",
        individual_table: str = "supply.egon_individual_heating",
        severity: str = "ERROR",
        rtol: float = 1e-6,
        **kwargs,
    ):
        """
        Parameters
        ----------
        technologies : Sequence[str]
            eTraGo carriers of :data:`TECHNOLOGIES` to check.
        link_table, generator_table : str
            eTraGo tables of the links and generators.
        district_table, individual_table : str
            Source tables of central and individual heat supply.
        severity : str
            Severity of a deviation, "ERROR" or "WARNING".
        rtol : float
            Relative tolerance per technology (default: 1e-6, i.e.
            rounding only).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            technologies=list(technologies),
            link_table=link_table,
            generator_table=generator_table,
            district_table=district_table,
            individual_table=individual_table,
            severity=severity,
            rtol=rtol,
            **kwargs,
        )
        self.link_table = link_table
        self.generator_table = generator_table
        self.district_table = district_table
        self.individual_table = individual_table

    def get_query(self, ctx):
        sources = {
            "district": self.district_table,
            "individual": self.individual_table,
        }
        components = {
            "link": (self.link_table, "bus1"),
            "generator": (self.generator_table, "bus"),
        }
        rows = []
        for carrier, component, source, source_carrier in TECHNOLOGIES:
            if carrier not in self.params["technologies"]:
                continue
            table, bus_column = components[component]
            capacity = (
                "c.p_nom * c.efficiency"
                if carrier.endswith("gas_boiler")
                else "c.p_nom"
            )
            rows.append(f"""
            SELECT
                '{carrier}' AS carrier,
                (
                    SELECT sum({capacity})
                    FROM {table} c
                    JOIN heat_buses b ON b.bus_id = c.{bus_column}
                    WHERE c.scn_name = :scenario
                    AND c.carrier = '{carrier}'
                ) AS observed,
                (
                    SELECT sum(capacity)
                    FROM {sources[source]}
                    WHERE scenario = :scenario
                    AND carrier = '{source_carrier}'
                ) AS expected
            """)
        return f"""
        WITH {self._heat_buses_cte()}
        SELECT
            (SELECT count(*) FROM heat_buses) AS n_heat_buses,
            t.carrier,
            t.observed,
            t.expected
        FROM ({" UNION ALL ".join(rows)}) t
        """

    def evaluate_df(self, df, ctx):
        if int(df["n_heat_buses"].values[0]) == 0:
            return self._no_heat_buses()

        rtol = float(self.params.get("rtol", 1e-6))
        checked = []
        deviations = []
        for row in df.itertuples():
            observed = 0.0 if pd.isna(row.observed) else float(row.observed)
            expected = 0.0 if pd.isna(row.expected) else float(row.expected)
            if observed == 0 and expected == 0:
                continue
            checked.append(row.carrier)
            if abs(observed - expected) > rtol * max(abs(expected), 1.0):
                deviation = (
                    f"{(observed - expected) / expected * 100:+.1f} %"
                    if expected
                    else "no source capacity"
                )
                deviations.append(
                    f"{row.carrier}: {observed:.1f} MW vs {expected:.1f} MW "
                    f"({deviation})"
                )

        if not checked:
            return self._skip(
                f"Scenario '{self.scenario}' has no capacity of "
                f"{', '.join(self.params['technologies'])}; check skipped"
            )
        if deviations:
            severity = Severity[self.params.get("severity", "ERROR")]
            return self._result(
                success=False,
                observed=len(deviations),
                expected=0,
                severity=severity,
                message=(
                    f"Scenario '{self.scenario}': heat supply capacity "
                    f"differs from the source tables for "
                    f"{'; '.join(deviations)}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': heat supply capacity matches "
                f"the source tables for {', '.join(checked)}"
            ),
        )


class HeatBuses(_HeatRule):
    """
    Check the number of central and rural heat buses.

    ``insert_buses`` creates one ``central_heat`` bus per district heating
    area of the scenario, and one ``rural_heat`` bus per MV grid district
    with heat demand outside district heating areas.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        district_heating_areas_table: str = (
            "demand.egon_district_heating_areas"
        ),
        map_grid_districts_table: str = (
            "boundaries.egon_map_zensus_grid_districts"
        ),
        heat_demand_table: str = "demand.egon_peta_heat",
        map_district_heating_areas_table: str = (
            "demand.egon_map_zensus_district_heating_areas"
        ),
        **kwargs,
    ):
        """
        Parameters
        ----------
        district_heating_areas_table : str
            District heating areas, one central heat bus each.
        map_grid_districts_table, heat_demand_table,
        map_district_heating_areas_table : str
            Tables to find the MV grid districts with heat demand outside
            district heating areas, one rural heat bus each.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            district_heating_areas_table=district_heating_areas_table,
            map_grid_districts_table=map_grid_districts_table,
            heat_demand_table=heat_demand_table,
            map_district_heating_areas_table=map_district_heating_areas_table,
            **kwargs,
        )

    def get_query(self, ctx):
        p = self.params
        return f"""
        WITH {self._heat_buses_cte()}
        SELECT
            (
                SELECT count(*) FROM heat_buses
                WHERE carrier = 'central_heat'
            ) AS central_buses,
            (
                SELECT count(*)
                FROM {p["district_heating_areas_table"]}
                WHERE scenario = :scenario
            ) AS central_expected,
            (
                SELECT count(*) FROM heat_buses
                WHERE carrier = 'rural_heat'
            ) AS rural_buses,
            (
                SELECT count(DISTINCT a.bus_id)
                FROM {p["map_grid_districts_table"]} a
                JOIN {p["heat_demand_table"]} d
                    ON d.zensus_population_id = a.zensus_population_id
                WHERE d.scenario = :scenario
                AND d.zensus_population_id NOT IN (
                    SELECT zensus_population_id
                    FROM {p["map_district_heating_areas_table"]}
                    WHERE scenario = :scenario
                )
            ) AS rural_expected
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        central, central_expected = (
            int(row["central_buses"]), int(row["central_expected"])
        )
        rural, rural_expected = (
            int(row["rural_buses"]), int(row["rural_expected"])
        )
        if central + rural == 0:
            return self._no_heat_buses()

        summary = (
            f"{central} central heat buses for {central_expected} district "
            f"heating areas, {rural} rural heat buses for {rural_expected} "
            f"MV grid districts with individual heat demand"
        )
        success = central == central_expected and rural == rural_expected
        return self._result(
            success=success,
            observed=central + rural,
            expected=central_expected + rural_expected,
            message=f"Scenario '{self.scenario}': {summary}",
        )


class HeatTimeseries(_HeatRule):
    """
    Check the time series of heat pumps and solar thermal.

    Every heat pump link needs exactly one row in the link time series
    table with its COP as ``efficiency``, and every solar thermal generator
    exactly one row in the generator time series table with ``p_max_pu``.
    Without them eTraGo would use a constant efficiency or availability.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        link_table: str = "grid.egon_etrago_link",
        link_timeseries_table: str = "grid.egon_etrago_link_timeseries",
        generator_table: str = "grid.egon_etrago_generator",
        generator_timeseries_table: str = (
            "grid.egon_etrago_generator_timeseries"
        ),
        **kwargs,
    ):
        """
        Parameters
        ----------
        link_table, link_timeseries_table : str
            eTraGo link table and its time series table.
        generator_table, generator_timeseries_table : str
            eTraGo generator table and its time series table.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            link_table=link_table,
            link_timeseries_table=link_timeseries_table,
            generator_table=generator_table,
            generator_timeseries_table=generator_timeseries_table,
            **kwargs,
        )

    def get_query(self, ctx):
        p = self.params
        return f"""
        WITH {self._heat_buses_cte()},
        heat_pumps AS (
            SELECT
                l.link_id,
                (
                    SELECT count(*)
                    FROM {p["link_timeseries_table"]} t
                    WHERE t.scn_name = l.scn_name
                    AND t.link_id = l.link_id
                    AND t.efficiency IS NOT NULL
                ) AS n_series
            FROM {p["link_table"]} l
            JOIN heat_buses b ON b.bus_id = l.bus1
            WHERE l.scn_name = :scenario
            AND l.carrier IN ('central_heat_pump', 'rural_heat_pump')
        ),
        solar_thermal AS (
            SELECT
                g.generator_id,
                (
                    SELECT count(*)
                    FROM {p["generator_timeseries_table"]} t
                    WHERE t.scn_name = g.scn_name
                    AND t.generator_id = g.generator_id
                    AND t.p_max_pu IS NOT NULL
                ) AS n_series
            FROM {p["generator_table"]} g
            JOIN heat_buses b ON b.bus_id = g.bus
            WHERE g.scn_name = :scenario
            AND g.carrier IN (
                'solar_thermal_collector', 'rural_solar_thermal'
            )
        )
        SELECT
            (SELECT count(*) FROM heat_buses) AS n_heat_buses,
            (SELECT count(*) FROM heat_pumps) AS n_heat_pumps,
            (
                SELECT count(*) FROM heat_pumps WHERE n_series <> 1
            ) AS heat_pumps_invalid,
            (SELECT count(*) FROM solar_thermal) AS n_solar_thermal,
            (
                SELECT count(*) FROM solar_thermal WHERE n_series <> 1
            ) AS solar_thermal_invalid
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        if int(row["n_heat_buses"]) == 0:
            return self._no_heat_buses()

        n_heat_pumps = int(row["n_heat_pumps"])
        n_solar = int(row["n_solar_thermal"])
        invalid_heat_pumps = int(row["heat_pumps_invalid"])
        invalid_solar = int(row["solar_thermal_invalid"])
        summary = (
            f"{n_heat_pumps - invalid_heat_pumps} of {n_heat_pumps} heat "
            f"pumps and {n_solar - invalid_solar} of {n_solar} solar thermal "
            f"generators have exactly one time series"
        )
        success = invalid_heat_pumps == 0 and invalid_solar == 0
        return self._result(
            success=success,
            observed=invalid_heat_pumps + invalid_solar,
            expected=0,
            message=f"Scenario '{self.scenario}': {summary}",
        )


class HeatStores(_HeatRule):
    """
    Check the extendable heat stores attached to every heat bus.

    In the future scenarios ``insert_store`` gives every central and rural
    heat bus one store bus with an extendable store, charger and
    discharger. The status quo scenarios have no heat stores by design, so
    for them ``stores_expected`` is false and any store is an error.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        stores_expected: bool,
        link_table: str = "grid.egon_etrago_link",
        store_table: str = "grid.egon_etrago_store",
        **kwargs,
    ):
        """
        Parameters
        ----------
        stores_expected : bool
            Whether the scenario gets heat stores.
        link_table, store_table : str
            eTraGo tables of the charger/discharger links and the stores.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            stores_expected=stores_expected,
            link_table=link_table,
            store_table=store_table,
            **kwargs,
        )

    def get_query(self, ctx):
        p = self.params
        rows = []
        for carrier in HEAT_BUS_CARRIERS:
            rows.append(f"""
            SELECT
                '{carrier}' AS carrier,
                (
                    SELECT count(*) FROM heat_buses
                    WHERE carrier = '{carrier}'
                ) AS n_heat_buses,
                (
                    SELECT count(*) FROM {self.bus_table}
                    WHERE scn_name = :scenario
                    AND country = 'DE'
                    AND carrier = '{carrier}_store'
                ) AS n_store_buses,
                (
                    SELECT count(*) FROM {p["store_table"]}
                    WHERE scn_name = :scenario
                    AND carrier = '{carrier}_store'
                ) AS n_stores,
                (
                    SELECT count(*) FROM {p["store_table"]}
                    WHERE scn_name = :scenario
                    AND carrier = '{carrier}_store'
                    AND NOT e_nom_extendable
                ) AS n_stores_fixed,
                (
                    SELECT count(*) FROM {p["link_table"]}
                    WHERE scn_name = :scenario
                    AND carrier = '{carrier}_store_charger'
                ) AS n_chargers,
                (
                    SELECT count(*) FROM {p["link_table"]}
                    WHERE scn_name = :scenario
                    AND carrier = '{carrier}_store_discharger'
                ) AS n_dischargers,
                (
                    SELECT count(*) FROM {p["link_table"]}
                    WHERE scn_name = :scenario
                    AND carrier IN (
                        '{carrier}_store_charger',
                        '{carrier}_store_discharger'
                    )
                    AND NOT p_nom_extendable
                ) AS n_links_fixed
            """)
        return f"""
        WITH {self._heat_buses_cte()}
        {" UNION ALL ".join(rows)}
        """

    def evaluate_df(self, df, ctx):
        if int(df["n_heat_buses"].sum()) == 0:
            return self._no_heat_buses()

        expected_stores = bool(self.params["stores_expected"])
        problems = []
        summary = []
        for row in df.itertuples():
            n_expected = row.n_heat_buses if expected_stores else 0
            counts = {
                "store buses": row.n_store_buses,
                "stores": row.n_stores,
                "chargers": row.n_chargers,
                "dischargers": row.n_dischargers,
            }
            wrong = {
                name: n for name, n in counts.items() if n != n_expected
            }
            if wrong:
                problems.append(
                    f"{row.carrier}: "
                    + ", ".join(f"{n} {name}" for name, n in wrong.items())
                    + f" for {n_expected} expected"
                )
            if row.n_stores_fixed or row.n_links_fixed:
                problems.append(
                    f"{row.carrier}: {row.n_stores_fixed} stores and "
                    f"{row.n_links_fixed} charger/discharger links are not "
                    f"extendable"
                )
            summary.append(f"{row.n_stores} {row.carrier} stores")

        if problems:
            return self._result(
                success=False,
                observed=len(problems),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': heat stores do not match "
                    f"the heat buses: {'; '.join(problems)}"
                ),
            )
        if not expected_stores:
            message = (
                f"Scenario '{self.scenario}': no heat stores, as intended "
                f"for this scenario"
            )
        else:
            message = (
                f"Scenario '{self.scenario}': one extendable store, charger "
                f"and discharger per heat bus ({', '.join(summary)})"
            )
        return self._result(success=True, observed=0, expected=0,
                            message=message)


class HeatScenarioCoverage(DataFrameRule):
    """
    Check that the German heat buses cover exactly the run's scenarios.

    A configured scenario without heat buses means the dataset wrote
    nothing for it; a scenario that is not configured means buses are left
    over from a run with a different ``--scenarios``. The configured
    scenarios are read when the rule runs, not when the DAG is built.
    """

    def __init__(self, table: str, rule_id: str, **kwargs):
        """
        Parameters
        ----------
        table : str
            eTraGo bus table, "grid.egon_etrago_bus".
        rule_id : str
            Unique identifier for this validation rule.
        """
        super().__init__(rule_id=rule_id, table=table, **kwargs)
        self.kind = "sanity"

    def get_query(self, ctx):
        carriers = ", ".join(f"'{c}'" for c in HEAT_BUS_CARRIERS)
        return f"""
        SELECT array_agg(DISTINCT scn_name) AS scenarios
        FROM {self.table}
        WHERE country = 'DE'
        AND carrier IN ({carriers})
        """

    def evaluate_df(self, df, ctx):
        observed = set(df["scenarios"].values[0] or [])
        expected = set(
            egon.data.config.settings()["egon-data"]["--scenarios"]
        )
        missing = sorted(expected - observed)
        extra = sorted(observed - expected)
        success = not missing and not extra

        if success:
            message = (
                f"Heat buses exist for exactly the configured scenarios: "
                f"{', '.join(sorted(observed))}"
            )
        else:
            message = (
                f"Configured scenarios without heat buses: "
                f"{', '.join(missing) or 'none'}; scenarios with heat buses "
                f"that are not configured: {', '.join(extra) or 'none'}"
            )
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=len(observed),
            expected=len(expected),
            message=message,
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )

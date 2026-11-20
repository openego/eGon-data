"""Sanity checks for electrical neighbours."""

from math import isclose
from pathlib import Path

import pandas as pd
from egon_validation.rules.base import DataFrameRule, RuleResult, Severity


class ElectricalNeighboursGrid(DataFrameRule):
    """Check foreign AC buses and their electrical connections."""

    def __init__(self, rule_id: str, scenario: str, **kwargs):
        super().__init__(
            rule_id=rule_id,
            table="grid.egon_etrago_line",
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
        WITH foreign_buses AS (
            SELECT bus_id
            FROM grid.egon_etrago_bus
            WHERE scn_name = :scenario AND carrier = 'AC'
              AND country IS NOT NULL AND country != 'DE'
        ),
        branches AS (
            SELECT bus0, bus1, s_nom AS capacity
            FROM grid.egon_etrago_line l
            WHERE l.scn_name = :scenario AND EXISTS (
                SELECT 1 FROM foreign_buses b
                WHERE b.bus_id IN (l.bus0, l.bus1)
            )
            UNION ALL
            SELECT bus0, bus1, p_nom AS capacity
            FROM grid.egon_etrago_link l
            WHERE l.scn_name = :scenario AND l.carrier = 'DC'
              AND EXISTS (
                SELECT 1 FROM foreign_buses b
                WHERE b.bus_id IN (l.bus0, l.bus1)
            )
            UNION ALL
            SELECT bus0, bus1, s_nom AS capacity
            FROM grid.egon_etrago_transformer t
            WHERE t.scn_name = :scenario AND EXISTS (
                SELECT 1 FROM foreign_buses b
                WHERE b.bus_id IN (t.bus0, t.bus1)
            )
        ),
        checked AS (
            SELECT br.*, b0.country AS country0, b1.country AS country1,
                   b0.bus_id AS id0, b1.bus_id AS id1
            FROM branches br
            LEFT JOIN grid.egon_etrago_bus b0
                ON b0.scn_name = :scenario AND b0.bus_id = br.bus0
            LEFT JOIN grid.egon_etrago_bus b1
                ON b1.scn_name = :scenario AND b1.bus_id = br.bus1
        )
        SELECT
            (SELECT count(*) FROM foreign_buses) AS n_buses,
            (SELECT count(*) FROM checked) AS n_branches,
            (SELECT count(*) FROM checked
             WHERE id0 IS NULL OR id1 IS NULL OR bus0 = bus1
                OR capacity IS NULL OR capacity <= 0) AS n_invalid,
            (SELECT count(*) FROM foreign_buses b
             WHERE NOT EXISTS (
                SELECT 1 FROM branches br
                WHERE b.bus_id IN (br.bus0, br.bus1)
             )) AS n_isolated,
            (SELECT count(*) FROM checked
             WHERE (country0 = 'DE' AND country1 != 'DE')
                OR (country1 = 'DE' AND country0 != 'DE')) AS n_cross_border
        """

    def evaluate_df(self, df, ctx):
        del ctx
        row = df.iloc[0]
        n_bad = (int(row.n_invalid) + int(row.n_isolated)
                 + int(row.n_buses == 0) + int(row.n_branches == 0)
                 + int(row.n_cross_border == 0))
        success = (int(row.n_buses) > 0 and int(row.n_branches) > 0
                   and int(row.n_cross_border) > 0 and n_bad == 0)
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=n_bad,
            expected=0,
            message=(f"{self.scenario}: {row.n_buses} foreign AC buses, "
                     f"{row.n_branches} electrical branches, "
                     f"{row.n_cross_border} DE-border connections, "
                     f"{row.n_invalid} invalid branches, "
                     f"{row.n_isolated} isolated foreign buses"),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )


class ElectricalNeighboursCapacities(DataFrameRule):
    """Compare foreign generator and storage capacities with local sources."""

    def __init__(self, rule_id: str, scenario: str, **kwargs):
        super().__init__(
            rule_id=rule_id,
            table="grid.egon_etrago_generator",
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
        SELECT 'generator' AS component, b.country, g.carrier,
               sum(g.p_nom) AS capacity
        FROM grid.egon_etrago_generator g
        JOIN grid.egon_etrago_bus b
          ON b.scn_name = g.scn_name AND b.bus_id = g.bus
        WHERE g.scn_name = :scenario AND b.carrier = 'AC'
          AND b.country IS NOT NULL AND b.country != 'DE'
          AND g.carrier != 'CH4'
        GROUP BY b.country, g.carrier
        UNION ALL
        SELECT 'storage' AS component, b.country, s.carrier,
               sum(s.p_nom) AS capacity
        FROM grid.egon_etrago_storage s
        JOIN grid.egon_etrago_bus b
          ON b.scn_name = s.scn_name AND b.bus_id = s.bus
        WHERE s.scn_name = :scenario AND b.carrier = 'AC'
          AND b.country IS NOT NULL AND b.country != 'DE'
          AND s.carrier IN ('battery', 'pumped_hydro')
        GROUP BY b.country, s.carrier
        UNION ALL
        SELECT 'renewable_profiles', '', '', count(*)::float
        FROM grid.egon_etrago_generator g
        JOIN grid.egon_etrago_bus b
          ON b.scn_name = g.scn_name AND b.bus_id = g.bus
        LEFT JOIN grid.egon_etrago_generator_timeseries t
          ON t.scn_name = g.scn_name AND t.generator_id = g.generator_id
          AND t.temp_id = 1
        WHERE g.scn_name = :scenario AND b.carrier = 'AC'
          AND b.country IS NOT NULL AND b.country != 'DE'
          AND g.carrier IN ('solar', 'wind_onshore', 'wind_offshore')
          AND (t.generator_id IS NULL OR t.p_max_pu IS NULL
               OR cardinality(t.p_max_pu) != 8760
               OR EXISTS (
                   SELECT 1 FROM unnest(t.p_max_pu) AS v
                   WHERE v IS NULL OR v = 'NaN' OR v < 0 OR v > 1
               ))
        """

    def evaluate_df(self, df, ctx):
        del ctx
        from egon.data.datasets.electrical_neighbours import (
            calc_capacities,
            map_carriers_entsoe,
        )

        status = "status" in self.scenario
        invalid_profiles = int(df.loc[
            df.component == "renewable_profiles", "capacity"
        ].iloc[0])
        battery_rows = df[(df.component == "storage") & (df.carrier == "battery")]
        battery_missing = int(status and not (
            len(battery_rows) == 1 and battery_rows.iloc[0].country == "GB"
        ))
        actual = {
            (r.component, r.country, r.carrier): float(r.capacity)
            for r in df.itertuples()
            if r.component != "renewable_profiles"
            and not (status and r.component == "storage" and r.carrier == "battery")
        }

        try:
            if status:
                file_path = Path(
                    f"data_bundle_egon_data/entsoe/gen_entsoe_{self.scenario}.csv"
                )
                capacities = pd.read_csv(file_path, index_col="Index")
                grouped = capacities.T.groupby(map_carriers_entsoe()).sum().T
                expected = {}
                for source_country, values in grouped.iterrows():
                    country = source_country.split("_")[0]
                    for carrier, value in values.items():
                        if value <= 0:
                            continue
                        if carrier == "Hydro Pumped Storage":
                            key = ("storage", country, "pumped_hydro")
                        elif carrier == "geo_thermal":
                            continue
                        else:
                            key = ("generator", country, carrier)
                        expected[key] = expected.get(key, 0) + float(value)
            else:
                capacities = calc_capacities(self.scenario).copy()
                generator_carriers = {
                    "others", "wind_offshore", "wind_onshore", "solar",
                    "reservoir", "run_of_river", "lignite", "coal", "oil",
                    "nuclear",
                }
                expected = {}
                for r in capacities.itertuples(index=False):
                    # Columns include 'Node/Line', which cannot be attribute-accessed.
                    node, carrier, value = r[1], r[0], r[2]
                    if carrier in generator_carriers:
                        component = "generator"
                    elif carrier in ("battery", "pumped_hydro"):
                        component = "storage"
                    else:
                        continue
                    country = node[:2].replace("UK", "GB")
                    key = (component, country, carrier)
                    expected[key] = expected.get(key, 0) + float(value)
                actual = {
                    k: v for k, v in actual.items()
                    if k[0] == "storage" or k[2] in generator_carriers
                }
            mismatches = [
                key for key in (expected.keys() | actual.keys())
                if not isclose(actual.get(key, 0), expected.get(key, 0),
                               rel_tol=1e-6, abs_tol=1e-3)
            ]
            error = ""
        except FileNotFoundError as exc:
            mismatches = ["missing source"]
            error = f"; missing local source: {exc.filename}"

        n_bad = len(mismatches) + invalid_profiles + battery_missing
        success = n_bad == 0 and bool(expected if not error else False)
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=n_bad,
            expected=0,
            message=(f"{self.scenario}: {len(mismatches)} country/carrier "
                     f"capacity mismatches, {invalid_profiles} invalid renewable "
                     f"time series, {battery_missing} missing GB battery rows{error}; "
                     f"first mismatches: {mismatches[:5]}"),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )


class ElectricalNeighboursDemand(DataFrameRule):
    """Check foreign load profiles and compare annual demand with local sources."""

    def __init__(self, rule_id: str, scenario: str, **kwargs):
        super().__init__(
            rule_id=rule_id,
            table="grid.egon_etrago_load_timeseries",
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
        SELECT b.country, l.load_id, t.p_set
        FROM grid.egon_etrago_load l
        JOIN grid.egon_etrago_bus b
          ON b.scn_name = l.scn_name AND b.bus_id = l.bus
        LEFT JOIN grid.egon_etrago_load_timeseries t
          ON t.scn_name = l.scn_name AND t.load_id = l.load_id
          AND t.temp_id = 1
        WHERE l.scn_name = :scenario AND l.carrier = 'AC'
          AND b.carrier = 'AC' AND b.country IS NOT NULL
          AND b.country != 'DE'
        """

    def evaluate_df(self, df, ctx):
        del ctx
        from egon.data.datasets.electrical_neighbours import (
            _bracket_tyndp_years,
            get_map_buses,
            read_tyndp_demand,
            read_tyndp_ev_demand,
        )
        from egon.data.datasets.scenario_parameters import (
            align_weekdays,
            get_scenario_year,
            get_sector_parameters,
        )

        actual = {}
        invalid_profiles = 0
        for row in df.itertuples():
            values = row.p_set
            if (values is None or len(values) != 8760
                    or bool(pd.isna(values).any())):
                invalid_profiles += 1
                continue
            actual[row.country] = actual.get(row.country, 0) + sum(values)

        try:
            if "status" in self.scenario:
                year = int(self.scenario.split("status")[-1])
                file_path = Path(
                    f"data_bundle_egon_data/entsoe/load_entsoe_{self.scenario}.csv"
                )
                source = pd.read_csv(file_path, index_col="Index")
                source.index = pd.to_datetime(source.index)
                source = align_weekdays(
                    source, source_year=year,
                    target_year=get_sector_parameters("global", self.scenario)[
                        "weather_year"
                    ],
                )
                expected = {}
                for name in source.columns:
                    country = name.split("_")[0]
                    expected[country] = expected.get(country, 0) + float(
                        source[name].sum()
                    )
            else:
                year = get_scenario_year(self.scenario)
                weather_year = get_sector_parameters("global", self.scenario)[
                    "weather_year"
                ]
                nodes = (
                    "AT00", "BE00", "CH00", "CZ00", "DKE1", "DKW1",
                    "FR00", "NL00", "LUB1", "LUF1", "LUG1", "NOM1",
                    "NON1", "NOS0", "SE01", "SE02", "SE03", "SE04",
                    "PL00", "UK00", "UKNI",
                )
                lo, hi = _bracket_tyndp_years(
                    year, anchors=(2030, 2040, 2050)
                )
                weight = (year - lo) / (hi - lo)
                market_lo = read_tyndp_demand(lo, nodes, weather_year)
                market_hi = read_tyndp_demand(hi, nodes, weather_year)
                pros_lo = read_tyndp_demand(
                    lo, nodes, weather_year, "ELECTRICITY_PROSUMER"
                )
                pros_hi = read_tyndp_demand(
                    hi, nodes, weather_year, "ELECTRICITY_PROSUMER"
                )
                ev_lo, ev_hi = _bracket_tyndp_years(year)
                ev_weight = (year - ev_lo) / (ev_hi - ev_lo)
                source_ev_lo = read_tyndp_ev_demand(ev_lo)
                source_ev_hi = read_tyndp_ev_demand(ev_hi)
                ev = source_ev_lo.add(
                    source_ev_hi.sub(source_ev_lo, fill_value=0) * ev_weight,
                    fill_value=0,
                ).clip(lower=0)
                groups = {}
                mapping = get_map_buses()
                for node in nodes:
                    groups.setdefault(mapping.get(node, node), []).append(node)
                expected = {}
                for mapped_node, members in groups.items():
                    hourly = pd.Series(0.0, index=range(8760))
                    for node in members:
                        hourly += (market_lo[node]
                                   + (market_hi[node] - market_lo[node]) * weight)
                        if node in pros_lo:
                            hourly += pros_lo[node] * (1 - weight)
                        if node in pros_hi:
                            hourly += pros_hi[node] * weight
                        hourly += ev.get(node, 0.0) / 8760
                    country = mapped_node[:2].replace("UK", "GB")
                    expected[country] = expected.get(country, 0) + float(
                        hourly.clip(lower=0).iloc[:8760].sum()
                    )

            mismatches = [
                country for country in (expected.keys() | actual.keys())
                if country not in expected or country not in actual
                or not isclose(actual.get(country, 0), expected.get(country, 0),
                               rel_tol=1e-5, abs_tol=1.0)
            ]
            error = ""
        except FileNotFoundError as exc:
            mismatches = ["missing source"]
            error = f"; missing local source: {exc.filename}"

        n_bad = len(mismatches) + invalid_profiles
        success = bool(actual) and not n_bad
        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=n_bad,
            expected=0,
            message=(f"{self.scenario}: {len(df)} foreign loads, "
                     f"{invalid_profiles} invalid 8760-hour profiles, "
                     f"{len(mismatches)} country demand mismatches{error}; "
                     f"first mismatches: {mismatches[:5]}"),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )

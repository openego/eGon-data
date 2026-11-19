"""
Sanity check validation rules for the complete eTraGo bus table.

These rules check ``grid.egon_etrago_bus`` as eTraGo reads it: every bus
of every dataset, German and foreign, together with the branches (lines,
transformers, links) and one-port components attached to it. They run in
``FinalValidations`` and fall into three groups:

* all buses: attributes, a branch at every bus, connected AC, gas and
  hydrogen networks,
* Germany: a bus for every MV grid district,
* other countries: an AC bus in every neighbouring country.

Every per-scenario rule reports success with severity INFO for a scenario
that is absent from the table, because which scenarios a run produces
depends on ``--scenarios``.
"""

import numpy as np
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

from egon_validation.rules.base import Severity

from egon.data.validation.rules.custom.sanity.etrago_generator_table import (
    _GeneratorTableRule,
)
from egon.data.validation.rules.custom.sanity.etrago_generators import (
    _parse_json,
)
from egon.data.validation.rules.custom.sanity.etrago_load_table import (
    LoadTableScenarioCoverage,
)

#: Nominal voltages of the AC buses in kV.
AC_VOLTAGES = (110, 220, 380)

#: Countries neighbouring Germany that are modelled with their own buses.
NEIGHBOUR_COUNTRIES = (
    "AT", "BE", "CH", "CZ", "DK", "FR", "GB", "LU", "NL", "NO", "PL", "SE",
)

#: Bounding box (lon_min, lon_max, lat_min, lat_max) that every bus must lie
#: in. It covers Germany and all neighbours with margin, and catches swapped
#: or zeroed coordinates.
EUROPE_BOUNDS = (-15.0, 35.0, 34.0, 72.0)


class _BusTableRule(_GeneratorTableRule):
    """
    Common base for the per-scenario rules on the whole bus table.

    Subclasses return exactly one row from their query, because the
    framework reports an empty query result as a failure.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str,
        line_table: str = "grid.egon_etrago_line",
        transformer_table: str = "grid.egon_etrago_transformer",
        link_table: str = "grid.egon_etrago_link",
        **kwargs,
    ):
        """
        Parameters
        ----------
        table : str
            Bus table, "grid.egon_etrago_bus".
        rule_id : str
            Unique identifier for this validation rule.
        scenario : str
            Scenario to check, e.g. "status2024".
        line_table, transformer_table, link_table : str
            eTraGo branch tables.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            bus_table=table,
            line_table=line_table,
            transformer_table=transformer_table,
            link_table=link_table,
            **kwargs,
        )
        self.line_table = line_table
        self.transformer_table = transformer_table
        self.link_table = link_table

    def _no_buses(self):
        return self._skip(
            f"Scenario '{self.scenario}' has no buses in {self.table}; "
            f"check skipped"
        )


class BusTableScenarioCoverage(LoadTableScenarioCoverage):
    """
    Check that every configured scenario has buses.

    A configured scenario without buses is an error, a scenario that is
    not configured is a warning (see :class:`LoadTableScenarioCoverage`).
    """

    component = "buses"


class BusTableAttributes(_BusTableRule):
    """
    Check that the core attributes of every bus are set and consistent.

    An error is a bus without ``country`` or ``geom``, whose ``x``/``y``
    differ from ``geom``, that lies outside ``bounds`` (swapped or zeroed
    coordinates), or an AC bus whose ``v_nom`` is not one of
    ``ac_voltages``. Two AC buses with the same coordinates and ``v_nom``
    are only a warning: they are probably duplicates, but harmless for the
    optimisation.
    """

    def __init__(self, table, rule_id, scenario, ac_voltages=None,
                 bounds=None, **kwargs):
        """
        Parameters
        ----------
        ac_voltages : Sequence[float], optional
            Permitted nominal voltages of AC buses in kV (default:
            :data:`AC_VOLTAGES`).
        bounds : Sequence[float], optional
            lon_min, lon_max, lat_min, lat_max (default:
            :data:`EUROPE_BOUNDS`).
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            ac_voltages=list(ac_voltages or AC_VOLTAGES),
            bounds=list(bounds or EUROPE_BOUNDS),
            **kwargs,
        )

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        lon_min, lon_max, lat_min, lat_max = self.params["bounds"]
        return {
            "scenario": self.scenario,
            "ac_voltages": [float(v) for v in self.params["ac_voltages"]],
            "lon_min": lon_min,
            "lon_max": lon_max,
            "lat_min": lat_min,
            "lat_max": lat_max,
        }

    def get_query(self, ctx):
        return f"""
        WITH buses AS (
            SELECT * FROM {self.table} WHERE scn_name = :scenario
        )
        SELECT
            count(*) AS n_buses,
            count(*) FILTER (WHERE country IS NULL) AS n_no_country,
            (
                array_agg(bus_id ORDER BY bus_id)
                FILTER (WHERE country IS NULL)
            )[1:10] AS no_country,
            count(*) FILTER (WHERE geom IS NULL) AS n_no_geom,
            count(*) FILTER (
                WHERE geom IS NOT NULL
                AND (
                    x IS NULL OR y IS NULL
                    OR abs(x - ST_X(geom)) > 1e-6
                    OR abs(y - ST_Y(geom)) > 1e-6
                )
            ) AS n_xy_mismatch,
            count(*) FILTER (
                WHERE NOT (
                    x BETWEEN :lon_min AND :lon_max
                    AND y BETWEEN :lat_min AND :lat_max
                )
            ) AS n_out_of_bounds,
            count(*) FILTER (
                WHERE carrier = 'AC'
                AND (
                    v_nom IS NULL
                    OR NOT v_nom = ANY(CAST(:ac_voltages AS float8[]))
                )
            ) AS n_bad_v_nom,
            (
                SELECT count(*)
                FROM (
                    SELECT 1
                    FROM buses
                    WHERE carrier = 'AC'
                    GROUP BY round(x::numeric, 6), round(y::numeric, 6),
                        v_nom
                    HAVING count(*) > 1
                ) d
            ) AS n_duplicate_sites
        FROM buses
        """

    def evaluate_df(self, df, ctx):
        n_buses = int(df["n_buses"].values[0])
        if n_buses == 0:
            return self._no_buses()

        counts = {
            k: int(df[k].values[0])
            for k in (
                "n_no_country", "n_no_geom", "n_xy_mismatch",
                "n_out_of_bounds", "n_bad_v_nom",
            )
        }
        n_duplicates = int(df["n_duplicate_sites"].values[0])
        duplicate_note = (
            f"; {n_duplicates} sites hold more than one AC bus of the same "
            f"v_nom" if n_duplicates else ""
        )

        if any(counts.values()):
            no_country = (
                f" (bus_id {list(df['no_country'].values[0])})"
                if counts["n_no_country"] else ""
            )
            return self._result(
                success=False,
                observed=sum(counts.values()),
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': of {n_buses} buses, "
                    f"{counts['n_no_country']} have no country{no_country}, "
                    f"{counts['n_no_geom']} no geom, "
                    f"{counts['n_xy_mismatch']} x/y different from geom, "
                    f"{counts['n_out_of_bounds']} lie outside "
                    f"{self.params['bounds']} and {counts['n_bad_v_nom']} "
                    f"AC buses have a v_nom outside "
                    f"{self.params['ac_voltages']} kV{duplicate_note}"
                ),
            )
        if n_duplicates:
            return self._result(
                success=False,
                observed=n_duplicates,
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': all {n_buses} buses have "
                    f"valid attributes{duplicate_note}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_buses} buses have a "
                f"country, a geom matching x/y inside the bounds, and every "
                f"AC bus a standard v_nom"
            ),
        )


class BusTableConnected(_BusTableRule):
    """
    Check that every bus is attached to at least one branch.

    A bus without a line, transformer or link cannot exchange energy with
    the rest of the network. If a load or generator sits on it, that demand
    can never be served or that supply never used: an error. A bus that is
    empty or holds only stores or storage units is a warning, because it is
    harmless for the optimisation.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        load_table: str = "grid.egon_etrago_load",
        generator_table: str = "grid.egon_etrago_generator",
        store_table: str = "grid.egon_etrago_store",
        storage_table: str = "grid.egon_etrago_storage",
        **kwargs,
    ):
        """
        Parameters
        ----------
        load_table, generator_table, store_table, storage_table : str
            eTraGo one-port component tables.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            load_table=load_table,
            generator_table=generator_table,
            store_table=store_table,
            storage_table=storage_table,
            **kwargs,
        )
        self.load_table = load_table
        self.generator_table = generator_table
        self.store_table = store_table
        self.storage_table = storage_table

    def get_query(self, ctx):
        return f"""
        WITH branch_ends AS (
            SELECT bus0 AS bus FROM {self.line_table}
            WHERE scn_name = :scenario
            UNION SELECT bus1 FROM {self.line_table}
            WHERE scn_name = :scenario
            UNION SELECT bus0 FROM {self.transformer_table}
            WHERE scn_name = :scenario
            UNION SELECT bus1 FROM {self.transformer_table}
            WHERE scn_name = :scenario
            UNION SELECT bus0 FROM {self.link_table}
            WHERE scn_name = :scenario
            UNION SELECT bus1 FROM {self.link_table}
            WHERE scn_name = :scenario
        ),
        served AS (
            SELECT bus FROM {self.load_table} WHERE scn_name = :scenario
            UNION SELECT bus FROM {self.generator_table}
            WHERE scn_name = :scenario
        ),
        unconnected AS (
            SELECT
                b.bus_id,
                b.carrier,
                s.bus IS NOT NULL AS has_load_or_generator
            FROM {self.table} b
            LEFT JOIN branch_ends e ON e.bus = b.bus_id
            LEFT JOIN served s ON s.bus = b.bus_id
            WHERE b.scn_name = :scenario
            AND e.bus IS NULL
        )
        SELECT
            (
                SELECT count(*) FROM {self.table}
                WHERE scn_name = :scenario
            ) AS n_buses,
            (
                SELECT json_agg(
                    json_build_object(
                        'carrier', carrier,
                        'served', has_load_or_generator,
                        'n', n,
                        'examples', examples
                    )
                    ORDER BY has_load_or_generator DESC, carrier
                )
                FROM (
                    SELECT
                        carrier,
                        has_load_or_generator,
                        count(*) AS n,
                        (array_agg(bus_id ORDER BY bus_id))[1:5] AS examples
                    FROM unconnected
                    GROUP BY carrier, has_load_or_generator
                ) g
            ) AS groups
        """

    def evaluate_df(self, df, ctx):
        n_buses = int(df["n_buses"].values[0])
        if n_buses == 0:
            return self._no_buses()

        groups = _parse_json(df["groups"].values[0]) or []

        def describe(rows):
            return "; ".join(
                f"{g['n']} {g['carrier']} (e.g. bus_id {g['examples']})"
                for g in rows
            )

        served = [g for g in groups if g["served"]]
        empty = [g for g in groups if not g["served"]]
        n_served = sum(g["n"] for g in served)
        n_empty = sum(g["n"] for g in empty)

        if served:
            return self._result(
                success=False,
                observed=n_served,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_served} buses with a "
                    f"load or generator have no line, transformer or link: "
                    f"{describe(served)}"
                    + (f"; without a branch and without load or generator: "
                       f"{describe(empty)}" if empty else "")
                ),
            )
        if empty:
            return self._result(
                success=False,
                observed=n_empty,
                expected=0,
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': {n_empty} buses without "
                    f"load or generator have no line, transformer or link: "
                    f"{describe(empty)}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_buses} buses are "
                f"attached to a line, transformer or link"
            ),
        )


class BusTableNetwork(_BusTableRule):
    """
    Check that the buses of one carrier form a single connected network.

    The edges are the lines and transformers and the links whose two buses
    both have ``carrier`` -- for AC the HVDC links, for CH4 and H2_grid the
    pipelines. The largest connected group is the main network; every other
    group is an island. Conversion links (e.g. electrolysis) are not edges,
    so an island cannot be supplied through another carrier's network.

    With ``serious_islands=True`` an island carrying a load or generator is
    an error and an empty one a warning; otherwise every island is a
    warning (e.g. hydrogen pipeline sections not yet joined to the core
    network).
    """

    def __init__(self, table, rule_id, scenario, carrier: str,
                 serious_islands: bool = False,
                 load_table: str = "grid.egon_etrago_load",
                 generator_table: str = "grid.egon_etrago_generator",
                 **kwargs):
        """
        Parameters
        ----------
        carrier : str
            Bus carrier whose network is checked, e.g. "AC".
        serious_islands : bool
            Whether an island with a load or generator is an error.
        load_table, generator_table : str
            eTraGo tables used to tell islands with demand or supply apart.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            carrier=carrier,
            serious_islands=serious_islands,
            load_table=load_table,
            generator_table=generator_table,
            **kwargs,
        )
        self.load_table = load_table
        self.generator_table = generator_table

    def get_params(self, ctx):
        """Return query parameters for parameterized queries."""
        return {"scenario": self.scenario, "carrier": self.params["carrier"]}

    def get_query(self, ctx):
        edges = " UNION ALL ".join(
            f"""
            SELECT e.bus0, e.bus1
            FROM {table} e
            JOIN nodes n0 ON n0.bus_id = e.bus0
            JOIN nodes n1 ON n1.bus_id = e.bus1
            WHERE e.scn_name = :scenario
            """
            for table in (
                self.line_table, self.transformer_table, self.link_table,
            )
        )
        return f"""
        WITH nodes AS (
            SELECT
                b.bus_id,
                COALESCE(b.country, '?') AS country,
                EXISTS (
                    SELECT 1 FROM {self.load_table} l
                    WHERE l.scn_name = :scenario AND l.bus = b.bus_id
                ) OR EXISTS (
                    SELECT 1 FROM {self.generator_table} g
                    WHERE g.scn_name = :scenario AND g.bus = b.bus_id
                ) AS served
            FROM {self.table} b
            WHERE b.scn_name = :scenario
            AND b.carrier = :carrier
        ),
        edges AS ({edges})
        SELECT
            (SELECT array_agg(bus_id ORDER BY bus_id) FROM nodes) AS bus_ids,
            (
                SELECT array_agg(country ORDER BY bus_id) FROM nodes
            ) AS countries,
            (
                SELECT array_agg(served ORDER BY bus_id) FROM nodes
            ) AS served,
            (SELECT array_agg(bus0) FROM edges) AS bus0,
            (SELECT array_agg(bus1) FROM edges) AS bus1
        """

    def evaluate_df(self, df, ctx):
        carrier = self.params["carrier"]
        bus_ids = df["bus_ids"].values[0]
        if not bus_ids:
            return self._skip(
                f"Scenario '{self.scenario}' has no {carrier} buses; check "
                f"skipped"
            )

        bus_ids = np.asarray(bus_ids)
        countries = np.asarray(df["countries"].values[0])
        served = np.asarray(df["served"].values[0], dtype=bool)
        bus0 = np.asarray(df["bus0"].values[0] or [], dtype=np.int64)
        bus1 = np.asarray(df["bus1"].values[0] or [], dtype=np.int64)

        index = np.searchsorted(bus_ids, np.concatenate([bus0, bus1]))
        n = len(bus_ids)
        graph = coo_matrix(
            (
                np.ones(len(bus0)),
                (index[: len(bus0)], index[len(bus0):]),
            ),
            shape=(n, n),
        )
        n_groups, labels = connected_components(graph, directed=False)

        sizes = np.bincount(labels)
        main = int(np.argmax(sizes))
        islands = []
        for group in range(n_groups):
            if group == main:
                continue
            members = labels == group
            islands.append(
                {
                    "size": int(sizes[group]),
                    "served": bool(served[members].any()),
                    "countries": ",".join(sorted(set(countries[members]))),
                    "example": int(bus_ids[members][0]),
                }
            )
        islands.sort(key=lambda i: (-i["served"], -i["size"]))

        if not islands:
            return self._result(
                success=True,
                observed=1,
                expected=1,
                message=(
                    f"Scenario '{self.scenario}': all {n} {carrier} buses "
                    f"form one connected network"
                ),
            )

        n_island_buses = sum(i["size"] for i in islands)
        details = "; ".join(
            f"{i['size']} bus(es) in {i['countries']} (e.g. {i['example']})"
            f"{', with load or generator' if i['served'] else ''}"
            for i in islands[:10]
        )
        serious = self.params.get("serious_islands", False) and any(
            i["served"] for i in islands
        )
        return self._result(
            success=False,
            observed=n_groups,
            expected=1,
            severity=Severity.ERROR if serious else Severity.WARNING,
            message=(
                f"Scenario '{self.scenario}': the {carrier} buses form "
                f"{n_groups} separate networks; the main one holds "
                f"{int(sizes[main])} of {n} buses, {len(islands)} islands "
                f"hold {n_island_buses}: {details}"
                + (" ..." if len(islands) > 10 else "")
            ),
        )


class BusTableMvGridBuses(_BusTableRule):
    """
    Check that every MV grid district has its German AC bus.

    The households, CTS and industry demand and the distributed generation
    of a district are attached to the bus whose id the district carries.
    """

    def __init__(
        self,
        table,
        rule_id,
        scenario,
        mv_grid_table: str = "grid.egon_mv_grid_district",
        **kwargs,
    ):
        """
        Parameters
        ----------
        mv_grid_table : str
            MV grid district table with a ``bus_id`` per district.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            mv_grid_table=mv_grid_table,
            **kwargs,
        )
        self.mv_grid_table = mv_grid_table

    def get_query(self, ctx):
        return f"""
        SELECT
            (
                SELECT count(*) FROM {self.table}
                WHERE scn_name = :scenario
            ) AS n_buses,
            count(*) AS n_districts,
            count(*) FILTER (WHERE b.bus_id IS NULL) AS n_missing,
            (
                array_agg(m.bus_id ORDER BY m.bus_id)
                FILTER (WHERE b.bus_id IS NULL)
            )[1:20] AS examples
        FROM {self.mv_grid_table} m
        LEFT JOIN {self.table} b
            ON b.bus_id = m.bus_id
            AND b.scn_name = :scenario
            AND b.carrier = 'AC'
            AND b.country = 'DE'
        """

    def evaluate_df(self, df, ctx):
        if int(df["n_buses"].values[0]) == 0:
            return self._no_buses()

        n_districts = int(df["n_districts"].values[0])
        n_missing = int(df["n_missing"].values[0])
        if n_missing:
            return self._result(
                success=False,
                observed=n_missing,
                expected=0,
                message=(
                    f"Scenario '{self.scenario}': {n_missing} of "
                    f"{n_districts} MV grid districts have no German AC bus, "
                    f"bus_id {list(df['examples'].values[0])}"
                ),
            )
        return self._result(
            success=True,
            observed=0,
            expected=0,
            message=(
                f"Scenario '{self.scenario}': all {n_districts} MV grid "
                f"districts have their German AC bus"
            ),
        )


class BusTableNeighbourCoverage(_BusTableRule):
    """
    Check that every neighbouring country has an AC bus.

    The set of countries other than DE with an AC bus is compared with
    ``countries``. A missing neighbour is an error, because its exchange
    with Germany is then not modelled; an unexpected country is a warning.
    Pass ``countries=None`` (e.g. through ``resolve_boundary_dependence``
    for a regional dataset boundary) to skip the check.
    """

    def __init__(self, table, rule_id, scenario,
                 countries=NEIGHBOUR_COUNTRIES, **kwargs):
        """
        Parameters
        ----------
        countries : Sequence[str] or None
            Expected neighbouring countries (default:
            :data:`NEIGHBOUR_COUNTRIES`); None skips the check.
        """
        super().__init__(
            table=table,
            rule_id=rule_id,
            scenario=scenario,
            countries=countries,
            **kwargs,
        )

    def get_query(self, ctx):
        return f"""
        SELECT
            count(*) AS n_buses,
            array_agg(DISTINCT country) FILTER (
                WHERE carrier = 'AC' AND country <> 'DE'
            ) AS countries
        FROM {self.table}
        WHERE scn_name = :scenario
        """

    def evaluate_df(self, df, ctx):
        expected = self.params.get("countries")
        if expected is None:
            return self._skip(
                f"Scenario '{self.scenario}': no expected neighbours for "
                f"this dataset boundary; check skipped"
            )
        if int(df["n_buses"].values[0]) == 0:
            return self._no_buses()

        expected = set(expected)
        observed = set(df["countries"].values[0] or [])
        missing = sorted(expected - observed)
        extra = sorted(observed - expected)

        if missing:
            return self._result(
                success=False,
                observed=len(observed & expected),
                expected=len(expected),
                message=(
                    f"Scenario '{self.scenario}': neighbouring countries "
                    f"without an AC bus: {', '.join(missing)}"
                    + (f"; unexpected countries: {', '.join(extra)}"
                       if extra else "")
                ),
            )
        if extra:
            return self._result(
                success=False,
                observed=len(observed),
                expected=len(expected),
                severity=Severity.WARNING,
                message=(
                    f"Scenario '{self.scenario}': all neighbouring countries "
                    f"have an AC bus, and so do unexpected countries: "
                    f"{', '.join(extra)}"
                ),
            )
        return self._result(
            success=True,
            observed=len(observed),
            expected=len(expected),
            message=(
                f"Scenario '{self.scenario}': all {len(expected)} "
                f"neighbouring countries have an AC bus"
            ),
        )

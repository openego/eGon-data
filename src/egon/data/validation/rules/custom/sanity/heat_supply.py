"""
Sanity check validation rules for the heat supply tables
``supply.egon_district_heating`` and ``supply.egon_individual_heating``.

All rules work on every scenario present in the checked table, so which
scenarios are checked follows ``--scenarios`` without having to configure
one rule per scenario.
"""

from egon_validation.rules.base import DataFrameRule, Severity


class DistrictHeatingSupplyAreaReference(DataFrameRule):
    """
    Validate that every supply row references an existing district heating
    area of the same scenario.

    ``district_heating_id`` is meant to be ``id`` of
    ``demand.egon_district_heating_areas``. The plain foreign key on ``id``
    does not cover the scenario, so a row could point to an area of a
    different scenario without the database complaining. This rule joins on
    both ``id`` and ``scenario``.

    Args:
        table: Supply table (supply.egon_district_heating)
        rule_id: Unique identifier for this validation rule
        area_table: District heating area table
            (default: demand.egon_district_heating_areas)
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        area_table: str = "demand.egon_district_heating_areas",
        **kwargs,
    ):
        super().__init__(
            rule_id=rule_id, table=table, area_table=area_table, **kwargs
        )
        self.kind = "sanity"

    def get_query(self, ctx):
        return f"""
        SELECT
            s.scenario,
            count(*) AS n_rows,
            count(*) FILTER (WHERE a.id IS NULL) AS n_orphans
        FROM {self.table} AS s
        LEFT JOIN {self.params["area_table"]} AS a
        ON a.id = s.district_heating_id
        AND a.scenario = s.scenario
        GROUP BY s.scenario
        ORDER BY s.scenario
        """

    def evaluate_df(self, df, ctx):
        n_orphans = int(df["n_orphans"].sum())
        success = n_orphans == 0

        details = ", ".join(
            f"{row.scenario}: {int(row.n_orphans)}/{int(row.n_rows)}"
            for row in df.itertuples()
        )

        return self.create_result(
            success=success,
            observed=n_orphans,
            expected=0,
            message=(
                f"Rows without a matching district heating area "
                f"(id and scenario) in {self.params['area_table']}: "
                f"{n_orphans} ({details})"
            ),
            column="district_heating_id",
            severity=Severity.INFO if success else Severity.ERROR,
        )


class DistrictHeatingSupplyUniqueCarrier(DataFrameRule):
    """
    Validate that each district heating area has at most one row per carrier
    and exactly one gas boiler.

    ``heat_etrago`` reads solar thermal and geothermal plants indexed by
    ``district_heating_id``, so a second row of these carriers for the same
    area breaks the insert. Resistive heaters are summed per area downstream
    and may legitimately appear twice, because ``backup_resistive_heaters``
    adds a second row when the cascade does not reach the national target.
    Those carriers are passed as ``allow_duplicate_carriers``.

    Every area of a scenario gets a backup gas boiler, so the number of
    areas without one has to be zero. Only scenarios present in the supply
    table are compared.

    Args:
        table: Supply table (supply.egon_district_heating)
        rule_id: Unique identifier for this validation rule
        area_table: District heating area table
            (default: demand.egon_district_heating_areas)
        allow_duplicate_carriers: Carriers that may occur more than once
            per area (default: ["resistive_heater"])
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        area_table: str = "demand.egon_district_heating_areas",
        allow_duplicate_carriers=("resistive_heater",),
        **kwargs,
    ):
        super().__init__(
            rule_id=rule_id,
            table=table,
            area_table=area_table,
            allow_duplicate_carriers=list(allow_duplicate_carriers),
            **kwargs,
        )
        self.kind = "sanity"

    def get_query(self, ctx):
        return f"""
        WITH duplicates AS (
            SELECT scenario, count(*) AS n_duplicates
            FROM (
                SELECT scenario, district_heating_id, carrier
                FROM {self.table}
                WHERE NOT carrier = ANY(:allow_duplicate_carriers)
                GROUP BY scenario, district_heating_id, carrier
                HAVING count(*) > 1
            ) AS d
            GROUP BY scenario
        ),
        boilers AS (
            SELECT
                a.scenario,
                count(*) AS n_areas,
                count(*) FILTER (WHERE b.n IS NULL) AS n_without_boiler,
                count(*) FILTER (WHERE b.n > 1) AS n_multiple_boilers
            FROM {self.params["area_table"]} AS a
            LEFT JOIN (
                SELECT scenario, district_heating_id, count(*) AS n
                FROM {self.table}
                WHERE carrier = 'gas_boiler'
                GROUP BY scenario, district_heating_id
            ) AS b
            ON b.scenario = a.scenario
            AND b.district_heating_id = a.id
            WHERE a.scenario IN (SELECT DISTINCT scenario FROM {self.table})
            GROUP BY a.scenario
        )
        SELECT
            b.scenario,
            b.n_areas,
            coalesce(d.n_duplicates, 0) AS n_duplicates,
            b.n_without_boiler,
            b.n_multiple_boilers
        FROM boilers AS b
        LEFT JOIN duplicates AS d USING (scenario)
        ORDER BY b.scenario
        """

    def get_params(self, ctx):
        return {
            "allow_duplicate_carriers": self.params[
                "allow_duplicate_carriers"
            ]
        }

    def evaluate_df(self, df, ctx):
        n_duplicates = int(df["n_duplicates"].sum())
        n_without_boiler = int(df["n_without_boiler"].sum())
        n_multiple_boilers = int(df["n_multiple_boilers"].sum())
        n_violations = n_duplicates + n_without_boiler + n_multiple_boilers
        success = n_violations == 0

        details = "; ".join(
            f"{row.scenario}: {int(row.n_duplicates)} duplicated "
            f"(area, carrier), {int(row.n_without_boiler)} of "
            f"{int(row.n_areas)} areas without gas boiler, "
            f"{int(row.n_multiple_boilers)} with several"
            for row in df.itertuples()
        )

        return self.create_result(
            success=success,
            observed=n_violations,
            expected=0,
            message=(
                f"Uniqueness per (scenario, district_heating_id, carrier) "
                f"excluding {self.params['allow_duplicate_carriers']} and "
                f"one gas boiler per area: {n_violations} violations "
                f"({details})"
            ),
            severity=Severity.INFO if success else Severity.ERROR,
        )


class DistrictHeatingSupplyScenarioTarget(DataFrameRule):
    """
    Validate the distributed capacity per carrier against the national
    target in ``supply.egon_scenario_capacities``.

    The targets are stored as ``urban_central_<carrier>``. Only carriers with
    such a target are compared; CHP and gas boilers have none, and status quo
    scenarios have no urban_central targets at all, so they are reported as
    not checkable instead of failing. A target without any distributed
    capacity fails.

    Args:
        table: Supply table (supply.egon_district_heating)
        rule_id: Unique identifier for this validation rule
        capacity_table: Scenario capacity table
            (default: supply.egon_scenario_capacities)
        rtol: Relative tolerance (default: 0.01 = 1 %)
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        capacity_table: str = "supply.egon_scenario_capacities",
        rtol: float = 0.01,
        **kwargs,
    ):
        super().__init__(
            rule_id=rule_id,
            table=table,
            capacity_table=capacity_table,
            rtol=rtol,
            **kwargs,
        )
        self.kind = "sanity"

    def get_query(self, ctx):
        return f"""
        WITH scenarios AS (
            SELECT DISTINCT scenario FROM {self.table}
        ),
        targets AS (
            SELECT
                scenario_name AS scenario,
                substring(carrier FROM '^urban_central_(.*)$') AS carrier,
                sum(capacity) AS target
            FROM {self.params["capacity_table"]}
            WHERE carrier ~ '^urban_central_'
            GROUP BY scenario_name, carrier
        ),
        distributed AS (
            SELECT scenario, carrier, sum(capacity) AS distributed
            FROM {self.table}
            GROUP BY scenario, carrier
        )
        -- Scenarios without targets keep one row with a NULL carrier, so a
        -- status quo only run is reported instead of returning no rows.
        SELECT
            s.scenario,
            t.carrier,
            t.target,
            coalesce(d.distributed, 0) AS distributed
        FROM scenarios AS s
        LEFT JOIN targets AS t USING (scenario)
        LEFT JOIN distributed AS d USING (scenario, carrier)
        ORDER BY s.scenario, t.carrier
        """

    def evaluate_df(self, df, ctx):
        rtol = float(self.params.get("rtol", 0.01))

        unchecked = sorted(df.loc[df["carrier"].isna(), "scenario"].unique())
        df = df[df["carrier"].notna()]
        if df.empty:
            return self.create_result(
                success=True,
                message=(
                    f"No urban_central targets for scenarios {unchecked}; "
                    f"check skipped"
                ),
                severity=Severity.INFO,
            )
        skipped = (
            f"; no targets for {unchecked}, not checked" if unchecked else ""
        )

        # A zero target cannot give a relative deviation; it only has to
        # stay undistributed.
        df = df.assign(
            deviation=[
                abs(d - t) / abs(t) if t else (0.0 if not d else float("inf"))
                for d, t in zip(df["distributed"], df["target"])
            ]
        )
        violations = df[df["deviation"] > rtol]
        max_deviation = float(df["deviation"].max())
        success = violations.empty

        if success:
            message = (
                f"Distributed capacity matches urban_central targets for "
                f"{len(df)} (scenario, carrier) pairs in "
                f"{sorted(df['scenario'].unique())} (max deviation "
                f"{max_deviation:.3%}, tolerance {rtol:.2%}){skipped}"
            )
        else:
            details = "; ".join(
                f"{row.scenario}/{row.carrier}: {row.distributed:.1f} MW vs "
                f"target {row.target:.1f} MW ({row.deviation:.2%})"
                for row in violations.itertuples()
            )
            message = (
                f"{len(violations)} (scenario, carrier) pairs deviate from "
                f"their urban_central target by more than {rtol:.2%}: "
                f"{details}{skipped}"
            )

        return self.create_result(
            success=success,
            observed=max_deviation,
            expected=rtol,
            message=message,
            column="capacity",
            severity=Severity.INFO if success else Severity.ERROR,
        )


class DistrictHeatingSupplyChpCapacity(DataFrameRule):
    """
    Validate the CHP capacity per district heating area against the CHP
    plants assigned to it in ``supply.egon_chp_plants``.

    ``egon_chp_plants.district_heating_area_id`` holds the ``area_id`` of
    an area, the supply table holds its ``id``. The reference therefore
    joins the plants on ``area_id`` and ``scenario`` and sums their thermal
    capacity per ``id``. Every area must have the same CHP capacity in both
    tables; an area with CHP in only one of them counts as a mismatch.

    Args:
        table: Supply table (supply.egon_district_heating)
        rule_id: Unique identifier for this validation rule
        chp_table: CHP plant table (default: supply.egon_chp_plants)
        area_table: District heating area table
            (default: demand.egon_district_heating_areas)
        rtol: Relative tolerance per area (default: 0.001 = 0.1 %)
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        chp_table: str = "supply.egon_chp_plants",
        area_table: str = "demand.egon_district_heating_areas",
        rtol: float = 0.001,
        **kwargs,
    ):
        super().__init__(
            rule_id=rule_id,
            table=table,
            chp_table=chp_table,
            area_table=area_table,
            rtol=rtol,
            **kwargs,
        )
        self.kind = "sanity"

    def get_query(self, ctx):
        return f"""
        WITH plants AS (
            SELECT p.scenario, a.id, sum(p.th_capacity) AS reference
            FROM {self.params["chp_table"]} AS p
            JOIN {self.params["area_table"]} AS a
            ON a.area_id = p.district_heating_area_id
            AND a.scenario = p.scenario
            WHERE p.district_heating
            AND p.scenario IN (SELECT DISTINCT scenario FROM {self.table})
            GROUP BY p.scenario, a.id
        ),
        supply AS (
            SELECT scenario, district_heating_id AS id,
                sum(capacity) AS distributed
            FROM {self.table}
            WHERE carrier = 'CHP'
            GROUP BY scenario, district_heating_id
        )
        SELECT
            coalesce(p.scenario, s.scenario) AS scenario,
            coalesce(p.id, s.id) AS id,
            coalesce(p.reference, 0) AS reference,
            coalesce(s.distributed, 0) AS distributed
        FROM plants AS p
        FULL JOIN supply AS s USING (scenario, id)
        """

    def evaluate_df(self, df, ctx):
        rtol = float(self.params.get("rtol", 0.001))

        # Absolute floor of 1 kW so rounding on tiny areas is not reported
        tolerance = (df["reference"].abs() * rtol).clip(lower=1e-3)
        df = df.assign(
            mismatch=(df["distributed"] - df["reference"]).abs() > tolerance
        )
        n_mismatch = int(df["mismatch"].sum())
        success = n_mismatch == 0

        per_scenario = df.groupby("scenario").agg(
            n_areas=("id", "size"),
            n_mismatch=("mismatch", "sum"),
            reference=("reference", "sum"),
            distributed=("distributed", "sum"),
        )
        details = "; ".join(
            f"{scenario}: {int(row.n_mismatch)} of {int(row.n_areas)} areas, "
            f"{row.distributed:.0f} MW in supply vs "
            f"{row.reference:.0f} MW in {self.params['chp_table']}"
            for scenario, row in per_scenario.iterrows()
        )

        return self.create_result(
            success=success,
            observed=n_mismatch,
            expected=0,
            message=(
                f"Areas whose CHP capacity differs from the assigned plants "
                f"by more than {rtol:.1%}: {n_mismatch} ({details})"
            ),
            column="capacity",
            severity=Severity.INFO if success else Severity.ERROR,
        )


class IndividualHeatingSupplyHeatPumpTarget(DataFrameRule):
    """
    Validate the individual heat pump capacity against its targets in
    ``supply.egon_scenario_capacities``.

    ``individual_heating()`` distributes heat pumps on one of two levels:

    * per federal state, if the scenario has ``residential_rural_heat_pump``
      targets per NUTS-1 region (reGon scenarios),
    * nationally, from the ``rural_heat_pump`` total (status quo). For a
      Schleswig-Holstein run the code divides that total by 16, so the
      national target is multiplied by ``national_share``.

    The rule picks the level per scenario from the targets that exist.
    Federal states are only compared if at least one MV grid lies in them,
    so a run with a smaller dataset boundary is not failed for states it
    does not contain. Scenarios without any heat pump target are reported
    as not checked.

    Args:
        table: Supply table (supply.egon_individual_heating)
        rule_id: Unique identifier for this validation rule
        capacity_table: Scenario capacity table
            (default: supply.egon_scenario_capacities)
        map_table: MV grid to federal state mapping
            (default: boundaries.egon_map_mvgriddistrict_vg250)
        state_table: Federal states (default: boundaries.vg250_lan)
        national_share: Share of the national target distributed in this
            run (default: 1.0). Use resolve_boundary_dependence to set
            1/16 for Schleswig-Holstein.
        rtol: Relative tolerance (default: 0.01 = 1 %)
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        capacity_table: str = "supply.egon_scenario_capacities",
        map_table: str = "boundaries.egon_map_mvgriddistrict_vg250",
        state_table: str = "boundaries.vg250_lan",
        national_share=1.0,
        rtol: float = 0.01,
        **kwargs,
    ):
        super().__init__(
            rule_id=rule_id,
            table=table,
            capacity_table=capacity_table,
            map_table=map_table,
            state_table=state_table,
            national_share=national_share,
            rtol=rtol,
            **kwargs,
        )
        self.kind = "sanity"

    def get_query(self, ctx):
        return f"""
        WITH scenarios AS (
            SELECT DISTINCT scenario FROM {self.table}
        ),
        -- vg250_lan has extra rows such as 'Bayern (Bodensee)', the mapping
        -- uses the plain state names
        states AS (
            SELECT DISTINCT m.vg250_lan AS state, l.nuts
            FROM {self.params["map_table"]} AS m
            JOIN {self.params["state_table"]} AS l ON l.gen = m.vg250_lan
        ),
        state_targets AS (
            SELECT scenario_name AS scenario, nuts, sum(capacity) AS target
            FROM {self.params["capacity_table"]}
            WHERE carrier = 'residential_rural_heat_pump'
            GROUP BY scenario_name, nuts
        ),
        state_distributed AS (
            SELECT s.scenario, st.nuts, sum(s.capacity) AS distributed
            FROM {self.table} AS s
            JOIN {self.params["map_table"]} AS m ON m.bus_id = s.mv_grid_id
            JOIN states AS st ON st.state = m.vg250_lan
            WHERE s.carrier = 'heat_pump'
            GROUP BY s.scenario, st.nuts
        ),
        national_targets AS (
            SELECT scenario_name AS scenario, sum(capacity) AS target
            FROM {self.params["capacity_table"]}
            WHERE carrier = 'rural_heat_pump'
            GROUP BY scenario_name
        ),
        national_distributed AS (
            SELECT scenario, sum(capacity) AS distributed
            FROM {self.table}
            WHERE carrier = 'heat_pump'
            GROUP BY scenario
        )
        SELECT
            sc.scenario, 'state' AS level, st.state AS region,
            t.target, coalesce(d.distributed, 0) AS distributed
        FROM scenarios AS sc
        JOIN state_targets AS t ON t.scenario = sc.scenario
        JOIN (SELECT DISTINCT state, nuts FROM states) AS st
          ON st.nuts = t.nuts
        LEFT JOIN state_distributed AS d
          ON d.scenario = sc.scenario AND d.nuts = t.nuts
        UNION ALL
        SELECT
            sc.scenario, 'national', 'Germany',
            t.target, coalesce(d.distributed, 0)
        FROM scenarios AS sc
        JOIN national_targets AS t ON t.scenario = sc.scenario
        LEFT JOIN national_distributed AS d ON d.scenario = sc.scenario
        WHERE sc.scenario NOT IN (SELECT scenario FROM state_targets)
        UNION ALL
        -- Scenarios without any target keep one row, so they are reported
        -- instead of returning no rows
        SELECT sc.scenario, NULL, NULL, NULL, 0
        FROM scenarios AS sc
        WHERE sc.scenario NOT IN (SELECT scenario FROM state_targets)
        AND sc.scenario NOT IN (SELECT scenario FROM national_targets)
        """

    def evaluate_df(self, df, ctx):
        rtol = float(self.params.get("rtol", 0.01))
        share = float(self.params.get("national_share", 1.0))

        unchecked = sorted(df.loc[df["level"].isna(), "scenario"].unique())
        df = df[df["level"].notna()].copy()
        skipped = (
            f"; no heat pump targets for {unchecked}, not checked"
            if unchecked
            else ""
        )
        if df.empty:
            return self.create_result(
                success=True,
                message=(
                    f"No heat pump targets for scenarios {unchecked}; "
                    f"check skipped"
                ),
                severity=Severity.INFO,
            )

        national = df["level"] == "national"
        df.loc[national, "target"] = df.loc[national, "target"] * share

        # A zero target cannot give a relative deviation; it only has to
        # stay undistributed.
        df["deviation"] = [
            abs(d - t) / abs(t) if t else (0.0 if not d else float("inf"))
            for d, t in zip(df["distributed"], df["target"])
        ]
        violations = df[df["deviation"] > rtol]
        max_deviation = float(df["deviation"].max())
        success = violations.empty

        levels = "; ".join(
            f"{scenario}: {grp['level'].iloc[0]} ({len(grp)} regions, "
            f"{grp['distributed'].sum():.0f} of {grp['target'].sum():.0f} MW)"
            for scenario, grp in df.groupby("scenario")
        )
        if success:
            message = (
                f"Heat pump capacity matches its targets ({levels}; max "
                f"deviation {max_deviation:.3%}, tolerance {rtol:.2%})"
                f"{skipped}"
            )
        else:
            details = "; ".join(
                f"{row.scenario}/{row.region}: {row.distributed:.1f} MW vs "
                f"target {row.target:.1f} MW ({row.deviation:.2%})"
                for row in violations.itertuples()
            )
            message = (
                f"{len(violations)} regions deviate from their heat pump "
                f"target by more than {rtol:.2%}: {details}{skipped}"
            )

        return self.create_result(
            success=success,
            observed=max_deviation,
            expected=rtol,
            message=message,
            column="capacity",
            severity=Severity.INFO if success else Severity.ERROR,
        )

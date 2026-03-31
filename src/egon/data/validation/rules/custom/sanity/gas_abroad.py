"""
Sanity check validation rules for gas sector components abroad.

Validates gas loads, generators, stores, and links for countries
outside Germany against reference data (TYNDP, SciGRID_gas).

Note: The original function etrago_eGon2035_gas_abroad() only logged
deviations without pass/fail logic. These rules add threshold-based
validation.
"""

from egon_validation.rules.base import Rule, RuleResult, Severity

from egon.data import config, db
from egon.data.datasets.gas_neighbours.eGon2035 import (
    calc_capacities,
    calc_ch4_storage_capacities,
    calc_global_ch4_demand,
    calc_global_power_to_h2_demand,
    calculate_ch4_grid_capacities,
    import_ch4_demandTS,
)

TESTMODE_OFF = (
    config.settings()["egon-data"]["--dataset-boundary"] == "Everything"
)


class GasBusesIsolatedAbroad(Rule):
    """
    Check for isolated gas buses abroad.

    Validates that all CH4 buses abroad are connected to at least one
    CH4 link (either as bus0 or bus1).

    Only runs when TESTMODE_OFF (boundary == "Everything").
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str = "eGon2035",
        carrier: str = "CH4",
        **kwargs,
    ):
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            carrier=carrier,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario
        self.carrier = carrier

    def evaluate(self, engine, ctx):
        """Check for isolated gas buses abroad."""
        if not TESTMODE_OFF:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                message="Testmode is on, skipping sanity check",
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        try:
            # Link carrier mapping (same as original)
            link_carrier = self.carrier  # CH4 -> CH4

            sql = f"""
            SELECT bus_id, carrier, country
            FROM grid.egon_etrago_bus
            WHERE scn_name = '{self.scenario}'
            AND carrier = '{self.carrier}'
            AND country != 'DE'
            AND bus_id NOT IN
                (SELECT bus0
                FROM grid.egon_etrago_link
                WHERE scn_name = '{self.scenario}'
                AND carrier = '{link_carrier}')
            AND bus_id NOT IN
                (SELECT bus1
                FROM grid.egon_etrago_link
                WHERE scn_name = '{self.scenario}'
                AND carrier = '{link_carrier}')
            """

            isolated_buses = db.select_dataframe(sql, warning=False)

            if isolated_buses.empty:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    observed=0,
                    expected=0,
                    message=(
                        f"No isolated {self.carrier} buses abroad for "
                        f"{self.scenario}"
                    ),
                    severity=Severity.INFO,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )
            else:
                bus_ids = isolated_buses["bus_id"].tolist()[:10]
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    observed=len(isolated_buses),
                    expected=0,
                    message=(
                        f"Found {len(isolated_buses)} isolated "
                        f"{self.carrier} buses abroad for "
                        f"{self.scenario}: {bus_ids}..."
                    ),
                    severity=Severity.WARNING,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )

        except Exception as e:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"Error checking isolated buses abroad: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class CH4LoadsAbroad(Rule):
    """
    Validate CH4 loads abroad against TYNDP reference data.

    Compares the total annual CH4 load abroad from the database
    against the global CH4 demand calculated from TYNDP data.

    Only runs when TESTMODE_OFF (boundary == "Everything").
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str = "eGon2035",
        rtol: float = 0.10,
        **kwargs,
    ):
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            rtol=rtol,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario

    def evaluate(self, engine, ctx):
        """Evaluate CH4 loads abroad against reference data."""
        if not TESTMODE_OFF:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                message="Testmode is on, skipping sanity check",
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        try:
            # Get reference data
            (
                Norway_global_demand_1y,
                normalized_ch4_demandTS,
            ) = import_ch4_demandTS()
            input_CH4_demand_abroad = calc_global_ch4_demand(
                Norway_global_demand_1y
            )
            expected = input_CH4_demand_abroad["GlobD_2035"].sum()

            # Get observed data from database
            sql = f"""
            SELECT (SUM(
                (SELECT SUM(p)
                FROM UNNEST(b.p_set) p)))::numeric as load_mwh
            FROM grid.egon_etrago_load a
            JOIN grid.egon_etrago_load_timeseries b
            ON (a.load_id = b.load_id)
            JOIN grid.egon_etrago_bus c
            ON (a.bus=c.bus_id)
            AND b.scn_name = '{self.scenario}'
            AND a.scn_name = '{self.scenario}'
            AND c.scn_name = '{self.scenario}'
            AND c.country != 'DE'
            AND a.carrier = 'CH4'
            """

            result = db.select_dataframe(sql, warning=False)
            observed = float(result["load_mwh"].values[0] or 0)

            if expected == 0:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    message="Expected CH4 demand abroad is zero",
                    severity=Severity.WARNING,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )

            rtol = self.params.get("rtol", 0.10)
            deviation = (observed - expected) / expected
            deviation_pct = deviation * 100
            success = abs(deviation) <= rtol

            if success:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    observed=observed,
                    expected=expected,
                    message=(
                        f"CH4 loads abroad for {self.scenario}: deviation "
                        f"{deviation_pct:.2f}% (tolerance: {rtol*100:.2f}%)"
                    ),
                    severity=Severity.INFO,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )
            else:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    observed=observed,
                    expected=expected,
                    message=(
                        f"CH4 loads abroad deviation for {self.scenario}: "
                        f"{deviation_pct:.2f}% (tolerance: {rtol*100:.2f}%)"
                    ),
                    severity=Severity.ERROR,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )

        except Exception as e:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"Error validating CH4 loads abroad: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class H2LoadsAbroad(Rule):
    """
    Validate H2_for_industry loads abroad against reference data.

    Compares the total H2_for_industry load abroad from the database
    against the global power-to-H2 demand calculation.

    Only runs when TESTMODE_OFF (boundary == "Everything").
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str = "eGon2035",
        rtol: float = 0.10,
        **kwargs,
    ):
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            rtol=rtol,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario

    def evaluate(self, engine, ctx):
        """Evaluate H2_for_industry loads abroad against reference data."""
        if not TESTMODE_OFF:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                message="Testmode is on, skipping sanity check",
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        try:
            # Get reference data
            input_power_to_h2_demand_abroad = calc_global_power_to_h2_demand()
            expected = input_power_to_h2_demand_abroad["GlobD_2035"].sum()

            # Get observed data from database
            sql = f"""
            SELECT SUM(p_set::numeric) as p_set_abroad
            FROM grid.egon_etrago_load
            WHERE scn_name = '{self.scenario}'
            AND carrier = 'H2_for_industry'
            AND bus IN
                (SELECT bus_id
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{self.scenario}'
                AND country != 'DE'
                AND carrier = 'AC')
            """

            result = db.select_dataframe(sql, warning=False)
            observed = float(result["p_set_abroad"].values[0] or 0)

            if expected == 0:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    message="Expected H2 demand abroad is zero",
                    severity=Severity.WARNING,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )

            rtol = self.params.get("rtol", 0.10)
            deviation = (observed - expected) / expected
            deviation_pct = deviation * 100
            success = abs(deviation) <= rtol

            if success:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    observed=observed,
                    expected=expected,
                    message=(
                        f"H2_for_industry loads abroad for {self.scenario}: "
                        f"deviation {deviation_pct:.2f}% "
                        f"(tolerance: {rtol*100:.2f}%)"
                    ),
                    severity=Severity.INFO,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )
            else:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    observed=observed,
                    expected=expected,
                    message=(
                        f"H2_for_industry loads abroad deviation for "
                        f"{self.scenario}: {deviation_pct:.2f}% "
                        f"(tolerance: {rtol*100:.2f}%)"
                    ),
                    severity=Severity.ERROR,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )

        except Exception as e:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"Error validating H2 loads abroad: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class CH4GeneratorsAbroad(Rule):
    """
    Validate CH4 generators abroad against TYNDP reference data.

    Compares the total nominal power of CH4 generators abroad from the database
    against the capacities calculated from TYNDP data.

    Only runs when TESTMODE_OFF (boundary == "Everything").
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str = "eGon2035",
        rtol: float = 0.10,
        **kwargs,
    ):
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            rtol=rtol,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario

    def evaluate(self, engine, ctx):
        """Evaluate CH4 generators abroad against reference data."""
        if not TESTMODE_OFF:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                message="Testmode is on, skipping sanity check",
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        try:
            # Get reference data
            CH4_gen = calc_capacities()
            expected = CH4_gen["cap_2035"].sum()

            # Get observed data from database
            sql = f"""
            SELECT SUM(p_nom::numeric) as p_nom_abroad
            FROM grid.egon_etrago_generator
            WHERE scn_name = '{self.scenario}'
            AND carrier = 'CH4'
            AND bus IN
                (SELECT bus_id
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{self.scenario}'
                AND country != 'DE'
                AND carrier = 'CH4')
            """

            result = db.select_dataframe(sql, warning=False)
            observed = float(result["p_nom_abroad"].values[0] or 0)

            if expected == 0:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    message="Expected CH4 generator capacity abroad is zero",
                    severity=Severity.WARNING,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )

            rtol = self.params.get("rtol", 0.10)
            deviation = (observed - expected) / expected
            deviation_pct = deviation * 100
            success = abs(deviation) <= rtol

            if success:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    observed=observed,
                    expected=expected,
                    message=(
                        f"CH4 generators abroad for {self.scenario}: "
                        f"deviation {deviation_pct:.2f}% "
                        f"(tolerance: {rtol*100:.2f}%)"
                    ),
                    severity=Severity.INFO,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )
            else:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    observed=observed,
                    expected=expected,
                    message=(
                        f"CH4 generators abroad deviation for "
                        f"{self.scenario}: {deviation_pct:.2f}% "
                        f"(tolerance: {rtol*100:.2f}%)"
                    ),
                    severity=Severity.ERROR,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )

        except Exception as e:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"Error validating CH4 generators abroad: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class CH4StoresAbroad(Rule):
    """
    Validate CH4 stores abroad against SciGRID_gas reference data.

    Compares the total storage capacity of CH4 stores abroad from the database
    against the capacities calculated from SciGRID_gas data.

    Only runs when TESTMODE_OFF (boundary == "Everything").
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str = "eGon2035",
        rtol: float = 0.10,
        **kwargs,
    ):
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            rtol=rtol,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario

    def evaluate(self, engine, ctx):
        """Evaluate CH4 stores abroad against reference data."""
        if not TESTMODE_OFF:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                message="Testmode is on, skipping sanity check",
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        try:
            # Get reference data
            ch4_input_capacities = calc_ch4_storage_capacities()
            expected = ch4_input_capacities["e_nom"].sum()

            # Get observed data from database
            sql = f"""
            SELECT SUM(e_nom::numeric) as e_nom_abroad
            FROM grid.egon_etrago_store
            WHERE scn_name = '{self.scenario}'
            AND carrier = 'CH4'
            AND bus IN
                (SELECT bus_id
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{self.scenario}'
                AND country != 'DE'
                AND carrier = 'CH4')
            """

            result = db.select_dataframe(sql, warning=False)
            observed = float(result["e_nom_abroad"].values[0] or 0)

            if expected == 0:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    message="Expected CH4 store capacity abroad is zero",
                    severity=Severity.WARNING,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )

            rtol = self.params.get("rtol", 0.10)
            deviation = (observed - expected) / expected
            deviation_pct = deviation * 100
            success = abs(deviation) <= rtol

            if success:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    observed=observed,
                    expected=expected,
                    message=(
                        f"CH4 stores abroad for {self.scenario}: "
                        f"deviation {deviation_pct:.2f}% "
                        f"(tolerance: {rtol*100:.2f}%)"
                    ),
                    severity=Severity.INFO,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )
            else:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    observed=observed,
                    expected=expected,
                    message=(
                        f"CH4 stores abroad deviation for {self.scenario}: "
                        f"{deviation_pct:.2f}% (tolerance: {rtol*100:.2f}%)"
                    ),
                    severity=Severity.ERROR,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )

        except Exception as e:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"Error validating CH4 stores abroad: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class CH4GridLinksAbroad(Rule):
    """
    Validate crossborder CH4 grid link capacity.

    Compares the total capacity of CH4 links connected to foreign buses
    against the CH4 grid capacities calculated from reference data.

    Only runs when TESTMODE_OFF (boundary == "Everything").
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str = "eGon2035",
        rtol: float = 0.10,
        **kwargs,
    ):
        super().__init__(
            rule_id=rule_id,
            table=table,
            scenario=scenario,
            rtol=rtol,
            **kwargs,
        )
        self.kind = "sanity"
        self.scenario = scenario

    def evaluate(self, engine, ctx):
        """Evaluate CH4 grid links abroad against reference data."""
        if not TESTMODE_OFF:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                message="Testmode is on, skipping sanity check",
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        try:
            # Get reference data
            ch4_grid_input_capacities = calculate_ch4_grid_capacities()
            expected = ch4_grid_input_capacities["p_nom"].sum()

            # Get observed data from database
            grid_carrier = "CH4"
            sql = f"""
            SELECT SUM(p_nom::numeric) as p_nom
            FROM grid.egon_etrago_link
            WHERE scn_name = '{self.scenario}'
            AND carrier = '{grid_carrier}'
            AND (bus0 IN
                (SELECT bus_id
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{self.scenario}'
                AND country != 'DE'
                AND carrier = '{grid_carrier}')
            OR bus1 IN
                (SELECT bus_id
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{self.scenario}'
                AND country != 'DE'
                AND carrier = '{grid_carrier}'))
            """

            result = db.select_dataframe(sql, warning=False)
            observed = float(result["p_nom"].values[0] or 0)

            if expected == 0:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    message="Expected CH4 grid capacity abroad is zero",
                    severity=Severity.WARNING,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )

            rtol = self.params.get("rtol", 0.10)
            deviation = (observed - expected) / expected
            deviation_pct = deviation * 100
            success = abs(deviation) <= rtol

            if success:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    observed=observed,
                    expected=expected,
                    message=(
                        f"CH4 grid links abroad for {self.scenario}: "
                        f"deviation {deviation_pct:.2f}% "
                        f"(tolerance: {rtol*100:.2f}%)"
                    ),
                    severity=Severity.INFO,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )
            else:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    observed=observed,
                    expected=expected,
                    message=(
                        f"CH4 grid links abroad deviation for "
                        f"{self.scenario}: {deviation_pct:.2f}% "
                        f"(tolerance: {rtol*100:.2f}%)"
                    ),
                    severity=Severity.ERROR,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )

        except Exception as e:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"Error validating CH4 grid links abroad: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

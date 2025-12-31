"""
Sanity check validation rules for gas grid components.

Validates gas bus connectivity, counts, and grid consistency.
"""

from pathlib import Path
import pandas as pd
from egon_validation.rules.base import DataFrameRule, RuleResult, Severity
from typing import List, Tuple
from egon.data.datasets.scenario_parameters import get_sector_parameters


class GasBusesIsolated(DataFrameRule):
    """
    Validate that gas buses are not isolated.

    Checks that all gas buses (CH4, H2_grid, H2_saltcavern) in Germany
    are connected to at least one link. Isolated buses indicate potential
    issues with grid connectivity.

    The check examines buses that don't appear in either bus0 or bus1
    of the corresponding link carrier.
    """

    def __init__(self, table: str, rule_id: str, scenario: str = "eGon2035",
                 carrier: str = "CH4", **kwargs):
        """
        Parameters
        ----------
        table : str
            Target table (grid.egon_etrago_bus)
        rule_id : str
            Unique identifier for this validation rule
        scenario : str
            Scenario name ("eGon2035" or "eGon100RE")
        carrier : str
            Bus carrier type ("CH4", "H2_grid", or "H2_saltcavern")
        """
        super().__init__(rule_id=rule_id, table=table, scenario=scenario,
                         carrier=carrier, **kwargs)
        self.kind = "sanity"
        self.scenario = scenario
        self.carrier = carrier

        # Map bus carrier to corresponding link carrier
        self.carrier_mapping = {
            "eGon2035": {
                "CH4": "CH4",
                "H2_grid": "H2_feedin",
                "H2_saltcavern": "power_to_H2",
            },
            "eGon100RE": {
                "CH4": "CH4",
                "H2_grid": "H2_retrofit",
                "H2_saltcavern": "H2_extension",
            }
        }

    def get_query(self, ctx):
        """
        Query to find isolated gas buses.

        Returns a query that finds buses of the specified carrier that
        are not connected to any links (don't appear in bus0 or bus1
        of links with the corresponding carrier).
        """
        if self.scenario not in self.carrier_mapping:
            # Return empty query for unsupported scenarios
            return "SELECT NULL as bus_id, NULL as carrier, NULL as country LIMIT 0"

        link_carrier = self.carrier_mapping[self.scenario].get(self.carrier)
        if not link_carrier:
            return "SELECT NULL as bus_id, NULL as carrier, NULL as country LIMIT 0"

        return f"""
        SELECT bus_id, carrier, country
        FROM grid.egon_etrago_bus
        WHERE scn_name = '{self.scenario}'
        AND carrier = '{self.carrier}'
        AND country = 'DE'
        AND bus_id NOT IN (
            SELECT bus0
            FROM grid.egon_etrago_link
            WHERE scn_name = '{self.scenario}'
            AND carrier = '{link_carrier}'
        )
        AND bus_id NOT IN (
            SELECT bus1
            FROM grid.egon_etrago_link
            WHERE scn_name = '{self.scenario}'
            AND carrier = '{link_carrier}'
        )
        """

    def evaluate_df(self, df, ctx):
        """
        Evaluate isolated buses.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with isolated buses (bus_id, carrier, country)
        ctx : dict
            Context information

        Returns
        -------
        RuleResult
            Validation result with success/failure status
        """
        # Filter out NULL rows from unsupported scenarios
        df = df.dropna()

        isolated_count = len(df)

        if isolated_count == 0:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=0,
                expected=0,
                message=(
                    f"No isolated {self.carrier} buses found for {self.scenario} "
                    f"(all buses connected to grid)"
                ),
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )
        else:
            # Get sample of isolated buses
            sample_buses = df.head(10)['bus_id'].tolist()

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=isolated_count,
                expected=0,
                message=(
                    f"Found {isolated_count} isolated {self.carrier} buses for {self.scenario} "
                    f"isolated_buses: {df.to_dict(orient='records')}"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )


class GasBusesCount(DataFrameRule):
    """
    Validate gas grid bus count against SciGRID_gas data.

    Compares the number of gas grid buses (CH4 or H2_grid) in the database
    against the original SciGRID_gas node count for Germany. Allows for
    small deviations due to grid simplification or modifications.
    """

    def __init__(self, table: str, rule_id: str, scenario: str = "eGon2035",
                 carrier: str = "CH4", rtol: float = 0.10, **kwargs):
        """
        Parameters
        ----------
        table : str
            Target table (grid.egon_etrago_bus)
        rule_id : str
            Unique identifier for this validation rule
        scenario : str
            Scenario name ("eGon2035" or "eGon100RE")
        carrier : str
            Bus carrier type ("CH4" or "H2_grid")
        rtol : float
            Relative tolerance for bus count deviation (default: 0.10 = 10%)
        """
        super().__init__(rule_id=rule_id, table=table, scenario=scenario,
                         carrier=carrier, rtol=rtol, **kwargs)
        self.kind = "sanity"
        self.scenario = scenario
        self.carrier = carrier

    def get_query(self, ctx):
        """
        Query to count gas grid buses in Germany.

        Returns a query that counts buses of the specified carrier
        in Germany for the specified scenario.
        """
        return f"""
        SELECT COUNT(*) as bus_count
        FROM grid.egon_etrago_bus
        WHERE scn_name = '{self.scenario}'
        AND country = 'DE'
        AND carrier = '{self.carrier}'
        """

    def evaluate_df(self, df, ctx):
        """
        Evaluate bus count against SciGRID_gas reference data.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with bus_count column
        ctx : dict
            Context information

        Returns
        -------
        RuleResult
            Validation result with success/failure status
        """
        if df.empty or df["bus_count"].isna().all():
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"No {self.carrier} buses found for scenario {self.scenario}",
                severity=Severity.WARNING,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

        observed_count = int(df["bus_count"].values[0])

        # Get expected count from SciGRID_gas data
        try:
            target_file = Path(".") / "datasets" / "gas_data" / "data" / "IGGIELGN_Nodes.csv"
            grid_buses_df = pd.read_csv(
                target_file,
                delimiter=";",
                decimal=".",
                usecols=["country_code"],
            )
            grid_buses_df = grid_buses_df[
                grid_buses_df["country_code"].str.match("DE")
            ]
            expected_count = len(grid_buses_df.index)
        except Exception as e:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"Error reading SciGRID_gas reference data: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

        # Calculate relative deviation
        rtol = self.params.get("rtol", 0.10)
        deviation = abs(observed_count - expected_count) / expected_count

        success = deviation <= rtol

        deviation_pct = deviation * 100

        if success:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=float(observed_count),
                expected=float(expected_count),
                message=(
                    f"{self.carrier} bus count valid for {self.scenario}: "
                    f"{observed_count} buses (deviation: {deviation_pct:.2f}%, "
                    f"tolerance: {rtol*100:.2f}%)"
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
                observed=float(observed_count),
                expected=float(expected_count),
                message=(
                    f"{self.carrier} bus count deviation too large for {self.scenario}: "
                    f"{observed_count} vs {expected_count} expected "
                    f"(deviation: {deviation_pct:.2f}%, tolerance: {rtol*100:.2f}%)"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )


class GasOnePortConnections(DataFrameRule):
    """
    Validate that gas one-port components are connected to existing buses.

    Checks that all gas one-port components (loads, generators, stores) are
    connected to buses that exist in the database with the correct carrier type.

    This validation ensures data integrity across the etrago tables and prevents
    orphaned components that would cause errors in network optimization.
    """

    def __init__(self, table: str, rule_id: str, scenario: str = "eGon2035",
                 component_type: str = "load", component_carrier: str = "CH4_for_industry",
                 bus_conditions: List[Tuple[str, str]] = None, **kwargs):
        """
        Parameters
        ----------
        table : str
            Target table (grid.egon_etrago_load, grid.egon_etrago_generator,
            or grid.egon_etrago_store)
        rule_id : str
            Unique identifier for this validation rule
        scenario : str
            Scenario name ("eGon2035" or "eGon100RE")
        component_type : str
            Type of component ("load", "generator", or "store")
        component_carrier : str
            Carrier of the component to check
        bus_conditions : List[Tuple[str, str]]
            List of (bus_carrier, country_condition) tuples that define valid buses
            Examples:
            - [("CH4", "= 'DE'")] - CH4 buses in Germany
            - [("CH4", "!= 'DE'")] - CH4 buses outside Germany
            - [("H2_grid", "= 'DE'"), ("AC", "!= 'DE'")] - H2_grid in DE OR AC abroad
        """
        super().__init__(rule_id=rule_id, table=table, scenario=scenario,
                         component_type=component_type,
                         component_carrier=component_carrier,
                         bus_conditions=bus_conditions or [], **kwargs)
        self.kind = "sanity"
        self.scenario = scenario
        self.component_type = component_type
        self.component_carrier = component_carrier
        self.bus_conditions = bus_conditions or []

        # Map component type to ID column name
        self.id_column_map = {
            "load": "load_id",
            "generator": "generator_id",
            "store": "store_id"
        }

    def get_query(self, ctx):
        """
        Query to find one-port components not connected to valid buses.

        Returns a query that finds components of the specified type and carrier
        that are NOT connected to any of the valid bus types specified in
        bus_conditions.
        """
        if not self.bus_conditions:
            # No bus conditions specified - skip validation
            return "SELECT NULL as component_id, NULL as bus, NULL as carrier LIMIT 0"

        id_column = self.id_column_map.get(self.component_type, "id")

        # Build bus subqueries for each condition
        bus_subqueries = []
        for bus_carrier, country_cond in self.bus_conditions:
            subquery = f"""
                (SELECT bus_id
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{self.scenario}'
                AND carrier = '{bus_carrier}'
                AND country {country_cond})
            """
            bus_subqueries.append(subquery)

        # Build NOT IN clauses for all bus conditions
        not_in_clauses = [f"bus NOT IN {subq}" for subq in bus_subqueries]
        combined_condition = " AND ".join(not_in_clauses)

        return f"""
        SELECT {id_column} as component_id, bus, carrier, scn_name
        FROM {self.table}
        WHERE scn_name = '{self.scenario}'
        AND carrier = '{self.component_carrier}'
        AND {combined_condition}
        """

    def evaluate_df(self, df, ctx):
        """
        Evaluate one-port component connections.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with disconnected components (component_id, bus, carrier)
        ctx : dict
            Context information

        Returns
        -------
        RuleResult
            Validation result with success/failure status
        """
        # Filter out NULL rows
        df = df.dropna()

        disconnected_count = len(df)

        if disconnected_count == 0:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=0,
                expected=0,
                message=(
                    f"All {self.component_carrier} {self.component_type}s connected "
                    f"to valid buses for {self.scenario}"
                ),
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )
        else:
            # Get sample of disconnected components
            sample_components = df.head(10)['component_id'].tolist()
            sample_buses = df.head(10)['bus'].tolist()

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=disconnected_count,
                expected=0,
                message=(
                    f"Found {disconnected_count} disconnected {self.component_carrier} "
                    f"{self.component_type}s for {self.scenario}. "
                    f"disconnected_components: {df.to_dict(orient='records')}, "
                    f"bus_conditions: {self.bus_conditions}"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )


class CH4GridCapacity(DataFrameRule):
    """
    Validate CH4 grid capacity against SciGRID_gas reference data.

    Compares the total capacity (p_nom) of CH4 pipelines in Germany from the
    database against the original SciGRID_gas pipeline data. For eGon100RE,
    the expected capacity is adjusted to account for the share of CH4 pipelines
    retrofitted to H2 pipelines (based on PyPSA-eur-sec parameters).

    This validation ensures that the CH4 grid capacity in the database matches
    the imported SciGRID_gas data, accounting for any scenario-specific modifications.
    """

    def __init__(self, table: str, rule_id: str, scenario: str = "eGon2035",
                 rtol: float = 0.10, **kwargs):
        """
        Parameters
        ----------
        table : str
            Target table (grid.egon_etrago_link)
        rule_id : str
            Unique identifier for this validation rule
        scenario : str
            Scenario name ("eGon2035" or "eGon100RE")
        rtol : float
            Relative tolerance for capacity deviation (default: 0.10 = 10%)
        """
        super().__init__(rule_id=rule_id, table=table, scenario=scenario,
                         rtol=rtol, **kwargs)
        self.kind = "sanity"
        self.scenario = scenario

    def get_query(self, ctx):
        """
        Query to get total CH4 pipeline capacity in Germany.

        Returns a query that sums the p_nom of all CH4 links where both
        bus0 and bus1 are in Germany.
        """
        return f"""
        SELECT SUM(p_nom::numeric) as total_p_nom
        FROM grid.egon_etrago_link
        WHERE scn_name = '{self.scenario}'
        AND carrier = 'CH4'
        AND bus0 IN (
            SELECT bus_id
            FROM grid.egon_etrago_bus
            WHERE scn_name = '{self.scenario}'
            AND country = 'DE'
            AND carrier = 'CH4'
        )
        AND bus1 IN (
            SELECT bus_id
            FROM grid.egon_etrago_bus
            WHERE scn_name = '{self.scenario}'
            AND country = 'DE'
            AND carrier = 'CH4'
        )
        """

    def _get_reference_capacity(self):
        """
        Calculate reference capacity from SciGRID_gas pipeline data.

        Returns
        -------
        float
            Expected total pipeline capacity for the scenario
        """
        try:
            # Read pipeline segments from SciGRID_gas
            target_file = (
                Path(".")
                / "datasets"
                / "gas_data"
                / "data"
                / "IGGIELGN_PipeSegments.csv"
            )

            pipelines = pd.read_csv(
                target_file,
                delimiter=";",
                decimal=".",
                usecols=["id", "node_id", "country_code", "param"],
            )

            # Parse bus0, bus1 and countries
            pipelines["bus0"] = pipelines["node_id"].apply(lambda x: x.split(",")[0])
            pipelines["bus1"] = pipelines["node_id"].apply(lambda x: x.split(",")[1])
            pipelines["country_0"] = pipelines["country_code"].apply(lambda x: x.split(",")[0])
            pipelines["country_1"] = pipelines["country_code"].apply(lambda x: x.split(",")[1])

            # Filter for pipelines within Germany
            germany_pipelines = pipelines[
                (pipelines["country_0"] == "DE") & (pipelines["country_1"] == "DE")
            ]

            # Read pipeline classification for capacity mapping
            classification_file = (
                Path(".")
                / "data_bundle_egon_data"
                / "pipeline_classification_gas"
                / "pipeline_classification.csv"
            )

            classification = pd.read_csv(
                classification_file,
                delimiter=",",
                usecols=["classification", "max_transport_capacity_Gwh/d"],
            )

            # Map pipeline param to capacity
            param_to_capacity = dict(
                zip(classification["classification"],
                    classification["max_transport_capacity_Gwh/d"])
            )

            germany_pipelines["p_nom"] = germany_pipelines["param"].map(param_to_capacity)

            # Sum total capacity
            total_p_nom = germany_pipelines["p_nom"].sum()

            # Adjust for eGon100RE (H2 retrofit share)
            if self.scenario == "eGon100RE":
                scn_params = get_sector_parameters("gas", "eGon100RE")
                h2_retrofit_share = scn_params["retrofitted_CH4pipeline-to-H2pipeline_share"]
                total_p_nom = total_p_nom * (1 - h2_retrofit_share)

            return float(total_p_nom)

        except Exception as e:
            raise ValueError(f"Error reading SciGRID_gas reference data: {str(e)}")

    def evaluate_df(self, df, ctx):
        """
        Evaluate CH4 grid capacity against reference data.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with total_p_nom column
        ctx : dict
            Context information

        Returns
        -------
        RuleResult
            Validation result with success/failure status
        """
        if df.empty or df["total_p_nom"].isna().all():
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=f"No CH4 links found for scenario {self.scenario}",
                severity=Severity.WARNING,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

        observed_capacity = float(df["total_p_nom"].values[0])

        # Get expected capacity from SciGRID_gas data
        try:
            expected_capacity = self._get_reference_capacity()
        except Exception as e:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                message=str(e),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

        # Calculate relative deviation
        rtol = self.params.get("rtol", 0.10)
        deviation = abs(observed_capacity - expected_capacity) / expected_capacity

        success = deviation <= rtol
        deviation_pct = deviation * 100

        if success:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=observed_capacity,
                expected=expected_capacity,
                message=(
                    f"CH4 grid capacity valid for {self.scenario}: "
                    f"{observed_capacity:.2f} GWh/d (deviation: {deviation_pct:.2f}%, "
                    f"tolerance: {rtol*100:.2f}%)"
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
                observed=observed_capacity,
                expected=expected_capacity,
                message=(
                    f"CH4 grid capacity deviation too large for {self.scenario}: "
                    f"{observed_capacity:.2f} vs {expected_capacity:.2f} GWh/d expected "
                    f"(deviation: {deviation_pct:.2f}%, tolerance: {rtol*100:.2f}%)"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )


class GasLinksConnections(DataFrameRule):
    """
    Validate that gas links are connected to existing buses.

    Checks that all gas links (two-port components) have both bus0 and bus1
    connected to buses that exist in the database. This validation ensures
    data integrity and prevents orphaned links that would cause errors in
    network optimization.

    This check covers all gas-related link carriers including CH4 pipelines,
    H2 conversion links, and power-to-gas links.
    """

    def __init__(self, table: str, rule_id: str, scenario: str = "eGon2035",
                 carrier: str = "CH4", **kwargs):
        """
        Parameters
        ----------
        table : str
            Target table (grid.egon_etrago_link)
        rule_id : str
            Unique identifier for this validation rule
        scenario : str
            Scenario name ("eGon2035" or "eGon100RE")
        carrier : str
            Link carrier type to check (e.g., "CH4", "H2_feedin", "power_to_H2")
        """
        super().__init__(rule_id=rule_id, table=table, scenario=scenario,
                         carrier=carrier, **kwargs)
        self.kind = "sanity"
        self.scenario = scenario
        self.carrier = carrier

    def get_query(self, ctx):
        """
        Query to find links with missing buses.

        Returns a query that finds links where either bus0 or bus1
        does not exist in the bus table for the same scenario.
        """
        return f"""
        SELECT link_id, bus0, bus1, carrier, scn_name
        FROM grid.egon_etrago_link
        WHERE scn_name = '{self.scenario}'
        AND carrier = '{self.carrier}'
        AND (
            bus0 NOT IN (
                SELECT bus_id
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{self.scenario}'
            )
            OR bus1 NOT IN (
                SELECT bus_id
                FROM grid.egon_etrago_bus
                WHERE scn_name = '{self.scenario}'
            )
        )
        """

    def evaluate_df(self, df, ctx):
        """
        Evaluate link connections.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with links that have missing buses
        ctx : dict
            Context information

        Returns
        -------
        RuleResult
            Validation result with success/failure status
        """
        disconnected_count = len(df)

        if disconnected_count == 0:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=0,
                expected=0,
                message=(
                    f"All {self.carrier} links connected to valid buses for {self.scenario}"
                ),
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )
        else:
            # Get sample of disconnected links
            sample_links = df.head(10)['link_id'].tolist()
            sample_bus0 = df.head(10)['bus0'].tolist()
            sample_bus1 = df.head(10)['bus1'].tolist()

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=disconnected_count,
                expected=0,
                message=(
                    f"Found {disconnected_count} disconnected {self.carrier} links "
                    f"for {self.scenario}. "
                    f"disconnected_links: {df.to_dict(orient='records')}"
                ),
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )

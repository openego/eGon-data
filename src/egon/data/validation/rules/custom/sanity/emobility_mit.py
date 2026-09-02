"""
Sanity check validation rules for eMobility: motorized individual travel.

Validates EV allocation, trip data, and model data in eTraGo tables.
"""

from egon_validation.rules.base import Rule, RuleResult, Severity
from sqlalchemy import Numeric
from sqlalchemy.sql import and_, cast, func, or_
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets import load_sources_and_targets
from egon.data.datasets.emobility.motorized_individual_travel.db_classes import (  # noqa: E501
    EgonEvCountMunicipality,
    EgonEvCountMvGridDistrict,
    EgonEvCountRegistrationDistrict,
    EgonEvMvGridDistrict,
    EgonEvPool,
    EgonEvTrip,
)
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    read_simbev_metadata_file,
)
from egon.data.datasets.etrago_setup import (
    EgonPfHvLink,
    EgonPfHvLinkTimeseries,
    EgonPfHvLoad,
    EgonPfHvLoadTimeseries,
    EgonPfHvStore,
    EgonPfHvStoreTimeseries,
)
from egon.data.datasets.scenario_parameters import get_sector_parameters

TESTMODE_OFF = (
    config.settings()["egon-data"]["--dataset-boundary"] == "Everything"
)


def get_scenario_variation_name(scenario: str) -> str:
    """Get the scenario variation name for a given scenario.

    Parameters
    ----------
    scenario : str
        Scenario name (e.g. "eGon2035", "eGon100RE")

    Returns
    -------
    str
        Scenario variation name (e.g. "NEP C 2035", "Reference 2050")
    """
    sources, _ = load_sources_and_targets("MotorizedIndividualTravel")
    return sources.files["original_data"]["scenario"]["variation"][scenario]


class EVAllocationCount(Rule):
    """
    Validate EV allocation counts against scenario targets.

    Checks that EV counts in count tables (Grid District, Municipality,
    Registration District) match the scenario target. Only validates
    when TESTMODE_OFF (boundary == "Everything").
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str = "eGon2035", **kwargs
    ):
        super().__init__(
            rule_id=rule_id, table=table, scenario=scenario, **kwargs
        )
        self.kind = "sanity"
        self.scenario = scenario

    def evaluate(self, engine, ctx):
        """Evaluate EV allocation counts."""
        if not TESTMODE_OFF:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                message="Testmode is on, skipping EV allocation count check",
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        try:
            scenario_var_name = get_scenario_variation_name(self.scenario)
            scenario_variation_parameters = get_sector_parameters(
                "mobility", scenario=self.scenario
            )["motorized_individual_travel"][scenario_var_name]
            ev_count_target = scenario_variation_parameters["ev_count"]

            errors = []
            with db.session_scope() as session:
                for table, level in zip(
                    [
                        EgonEvCountMvGridDistrict,
                        EgonEvCountMunicipality,
                        EgonEvCountRegistrationDistrict,
                    ],
                    ["Grid District", "Municipality", "Registration District"],
                ):
                    query = session.query(
                        func.sum(
                            table.bev_mini
                            + table.bev_medium
                            + table.bev_luxury
                            + table.phev_mini
                            + table.phev_medium
                            + table.phev_luxury
                        ).label("ev_count")
                    ).filter(
                        table.scenario == self.scenario,
                        table.scenario_variation == scenario_var_name,
                    )

                    ev_counts = pd.read_sql(
                        query.statement, query.session.bind, index_col=None
                    )
                    count = ev_counts.iloc[0].ev_count or 0

                    rtol = 0.0001 if ev_count_target > 1e6 else 0.02
                    if not np.allclose(count, ev_count_target, rtol=rtol):
                        errors.append(
                            f"{level}: {count} vs target {ev_count_target}"
                        )

            if errors:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    message=(
                        f"EV count mismatch for {self.scenario}: "
                        f"{'; '.join(errors)}"
                    ),
                    severity=Severity.ERROR,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=ev_count_target,
                expected=ev_count_target,
                message=f"EV allocation counts valid for {self.scenario}",
                severity=Severity.INFO,
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
                message=f"Error validating EV allocation: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class EVGridDistrictAllocation(Rule):
    """
    Validate EVs allocated to grid districts against scenario target.

    Only validates when TESTMODE_OFF (boundary == "Everything").
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str = "eGon2035", **kwargs
    ):
        super().__init__(
            rule_id=rule_id, table=table, scenario=scenario, **kwargs
        )
        self.kind = "sanity"
        self.scenario = scenario

    def evaluate(self, engine, ctx):
        """Evaluate EVs allocated to grid districts."""
        if not TESTMODE_OFF:
            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                message=(
                    "Testmode is on, skipping grid district allocation check"
                ),
                severity=Severity.INFO,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

        try:
            scenario_var_name = get_scenario_variation_name(self.scenario)
            scenario_variation_parameters = get_sector_parameters(
                "mobility", scenario=self.scenario
            )["motorized_individual_travel"][scenario_var_name]
            ev_count_target = scenario_variation_parameters["ev_count"]

            with db.session_scope() as session:
                query = session.query(
                    func.count(EgonEvMvGridDistrict.egon_ev_pool_ev_id).label(
                        "ev_count"
                    ),
                ).filter(
                    EgonEvMvGridDistrict.scenario == self.scenario,
                    EgonEvMvGridDistrict.scenario_variation
                    == scenario_var_name,
                )
            ev_count_alloc = (
                pd.read_sql(
                    query.statement, query.session.bind, index_col=None
                )
                .iloc[0]
                .ev_count
            )

            rtol = 0.0001 if ev_count_target > 1e6 else 0.02
            if np.allclose(ev_count_alloc, ev_count_target, rtol=rtol):
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    observed=ev_count_alloc,
                    expected=ev_count_target,
                    message=(
                        f"EVs allocated to grid districts valid for "
                        f"{self.scenario}"
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
                    observed=ev_count_alloc,
                    expected=ev_count_target,
                    message=(
                        f"EVs allocated to grid districts mismatch for "
                        f"{self.scenario}: {ev_count_alloc} vs "
                        f"{ev_count_target}"
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
                message=f"Error validating grid district allocation: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class EVTripTimeranges(Rule):
    """
    Validate EV trip timeranges.

    Checks that trips start at timestep 0 and have max 35040 steps
    (8760h in 15min steps).
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str = "eGon2035", **kwargs
    ):
        super().__init__(
            rule_id=rule_id, table=table, scenario=scenario, **kwargs
        )
        self.kind = "sanity"
        self.scenario = scenario

    def evaluate(self, engine, ctx):
        """Evaluate trip timeranges."""
        try:
            meta_run_config = read_simbev_metadata_file(
                self.scenario, "config"
            ).loc["basic"]

            with db.session_scope() as session:
                query = session.query(
                    func.count(EgonEvTrip.event_id).label("cnt")
                ).filter(
                    or_(
                        and_(
                            EgonEvTrip.park_start > 0,
                            EgonEvTrip.simbev_event_id == 0,
                        ),
                        EgonEvTrip.park_end
                        > (60 / int(meta_run_config.stepsize)) * 8760,
                    ),
                    EgonEvTrip.scenario == self.scenario,
                )
            invalid_trips = pd.read_sql(
                query.statement, query.session.bind, index_col=None
            )
            invalid_count = invalid_trips.iloc[0].cnt

            if invalid_count == 0:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    observed=0,
                    expected=0,
                    message=f"All trip timeranges valid for {self.scenario}",
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
                    observed=invalid_count,
                    expected=0,
                    message=(
                        f"{invalid_count} trips have invalid timesteps "
                        f"for {self.scenario}"
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
                message=f"Error validating trip timeranges: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class EVTripChargingDemand(Rule):
    """
    Validate EV trip charging demand vs available power.

    Checks that charging demand can be covered by available charging
    energy while parking.
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str = "eGon2035", **kwargs
    ):
        super().__init__(
            rule_id=rule_id, table=table, scenario=scenario, **kwargs
        )
        self.kind = "sanity"
        self.scenario = scenario

    def evaluate(self, engine, ctx):
        """Evaluate charging demand vs available power."""
        try:
            meta_run_config = read_simbev_metadata_file(
                self.scenario, "config"
            ).loc["basic"]

            with db.session_scope() as session:
                query = session.query(
                    func.count(EgonEvTrip.event_id).label("cnt")
                ).filter(
                    func.round(
                        cast(
                            (EgonEvTrip.park_end - EgonEvTrip.park_start + 1)
                            * EgonEvTrip.charging_capacity_nominal
                            * (int(meta_run_config.stepsize) / 60),
                            Numeric,
                        ),
                        3,
                    )
                    < cast(EgonEvTrip.charging_demand, Numeric),
                    EgonEvTrip.scenario == self.scenario,
                )
            invalid_trips = pd.read_sql(
                query.statement, query.session.bind, index_col=None
            )
            invalid_count = invalid_trips.iloc[0].cnt

            if invalid_count == 0:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    observed=0,
                    expected=0,
                    message=(
                        f"All trips have sufficient charging power "
                        f"for {self.scenario}"
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
                    observed=invalid_count,
                    expected=0,
                    message=(
                        f"{invalid_count} trips cannot cover charging demand "
                        f"with available power for {self.scenario}"
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
                message=f"Error validating charging demand: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class EVModelComponentsCreated(Rule):
    """
    Validate that all model components were created.

    Checks that grid districts with EVs have corresponding model
    components (load, store, link) in eTraGo tables.
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str = "eGon2035", **kwargs
    ):
        super().__init__(
            rule_id=rule_id, table=table, scenario=scenario, **kwargs
        )
        self.kind = "sanity"
        self.scenario = scenario

    def evaluate(self, engine, ctx):
        """Evaluate model components creation."""
        try:
            scenario_var_name = get_scenario_variation_name(self.scenario)

            # Get MVGDs which got EV allocated
            with db.session_scope() as session:
                query = (
                    session.query(
                        EgonEvMvGridDistrict.bus_id,
                    )
                    .filter(
                        EgonEvMvGridDistrict.scenario == self.scenario,
                        EgonEvMvGridDistrict.scenario_variation
                        == scenario_var_name,
                    )
                    .group_by(EgonEvMvGridDistrict.bus_id)
                )
            mvgds_with_ev = (
                pd.read_sql(
                    query.statement, query.session.bind, index_col=None
                )
                .bus_id.sort_values()
                .to_list()
            )

            # Load model components
            with db.session_scope() as session:
                query = (
                    session.query(
                        EgonPfHvLink.bus0.label("mvgd_bus_id"),
                        EgonPfHvLoad.bus.label("emob_bus_id"),
                        EgonPfHvLoad.load_id.label("load_id"),
                        EgonPfHvStore.store_id.label("store_id"),
                    )
                    .select_from(EgonPfHvLoad, EgonPfHvStore)
                    .join(
                        EgonPfHvLoadTimeseries,
                        EgonPfHvLoadTimeseries.load_id == EgonPfHvLoad.load_id,
                    )
                    .join(
                        EgonPfHvStoreTimeseries,
                        EgonPfHvStoreTimeseries.store_id
                        == EgonPfHvStore.store_id,
                    )
                    .filter(
                        EgonPfHvLoad.carrier == "land_transport_EV",
                        EgonPfHvLoad.scn_name == self.scenario,
                        EgonPfHvLoadTimeseries.scn_name == self.scenario,
                        EgonPfHvStore.carrier == "battery_storage",
                        EgonPfHvStore.scn_name == self.scenario,
                        EgonPfHvStoreTimeseries.scn_name == self.scenario,
                        EgonPfHvLink.scn_name == self.scenario,
                        EgonPfHvLink.bus1 == EgonPfHvLoad.bus,
                        EgonPfHvLink.bus1 == EgonPfHvStore.bus,
                    )
                )
            model_components = pd.read_sql(
                query.statement, query.session.bind, index_col=None
            )

            # Check number of buses with model components connected
            mvgd_buses_with_ev = model_components.loc[
                model_components.mvgd_bus_id.isin(mvgds_with_ev)
            ]

            errors = []

            if len(mvgds_with_ev) != len(mvgd_buses_with_ev):
                errors.append(
                    f"Grid districts mismatch: {len(mvgd_buses_with_ev)} with "
                    f"components vs {len(mvgds_with_ev)} with EVs allocated"
                )

            # Check if all required components exist (no NaN)
            if model_components.drop_duplicates().isna().any().any():
                missing = model_components.drop_duplicates().isna().any()
                errors.append(
                    f"Missing components: {missing[missing].index.tolist()}"
                )

            if errors:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    message=(
                        f"Model components incomplete for {self.scenario}: "
                        f"{'; '.join(errors)}"
                    ),
                    severity=Severity.ERROR,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=len(mvgds_with_ev),
                expected=len(mvgds_with_ev),
                message=(
                    f"All model components created for {self.scenario}: "
                    f"{len(mvgds_with_ev)} grid districts"
                ),
                severity=Severity.INFO,
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
                message=f"Error validating model components: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class EVModelTimeseriesLength(Rule):
    """
    Validate that all EV model timeseries have 8760 steps.
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str = "eGon2035", **kwargs
    ):
        super().__init__(
            rule_id=rule_id, table=table, scenario=scenario, **kwargs
        )
        self.kind = "sanity"
        self.scenario = scenario

    def evaluate(self, engine, ctx):
        """Evaluate timeseries lengths."""
        try:
            model_ts_dict = {
                "Load": {
                    "carrier": "land_transport_EV",
                    "table": EgonPfHvLoad,
                    "table_ts": EgonPfHvLoadTimeseries,
                    "column_id": "load_id",
                    "columns_ts": ["p_set"],
                },
                "Link": {
                    "carrier": "BEV_charger",
                    "table": EgonPfHvLink,
                    "table_ts": EgonPfHvLinkTimeseries,
                    "column_id": "link_id",
                    "columns_ts": ["p_max_pu"],
                },
                "Store": {
                    "carrier": "battery_storage",
                    "table": EgonPfHvStore,
                    "table_ts": EgonPfHvStoreTimeseries,
                    "column_id": "store_id",
                    "columns_ts": ["e_min_pu", "e_max_pu"],
                },
            }

            errors = []

            with db.session_scope() as session:
                for node, attrs in model_ts_dict.items():
                    subquery = (
                        session.query(
                            getattr(attrs["table"], attrs["column_id"])
                        )
                        .filter(attrs["table"].carrier == attrs["carrier"])
                        .filter(attrs["table"].scn_name == self.scenario)
                        .subquery()
                    )

                    cols = [
                        getattr(attrs["table_ts"], c)
                        for c in attrs["columns_ts"]
                    ]
                    query = session.query(
                        getattr(attrs["table_ts"], attrs["column_id"]), *cols
                    ).filter(
                        getattr(attrs["table_ts"], attrs["column_id"]).in_(
                            subquery
                        ),
                        attrs["table_ts"].scn_name == self.scenario,
                    )
                    ts_df = pd.read_sql(
                        query.statement,
                        query.session.bind,
                        index_col=attrs["column_id"],
                    )

                    for col in attrs["columns_ts"]:
                        invalid_ts = ts_df.loc[
                            ts_df[col].apply(lambda _: len(_)) != 8760
                        ][col].apply(len)
                        if len(invalid_ts) > 0:
                            errors.append(
                                f"{node}.{col}: {len(invalid_ts)} "
                                f"rows != 8760 steps"
                            )

            if errors:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=False,
                    message=(
                        f"Invalid timeseries lengths for {self.scenario}: "
                        f"{'; '.join(errors)}"
                    ),
                    severity=Severity.ERROR,
                    schema=self.schema,
                    table_name=self.table_name,
                    rule_class=self.__class__.__name__,
                )

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                message=(
                    f"All EV timeseries have 8760 steps for {self.scenario}"
                ),
                severity=Severity.INFO,
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
                message=f"Error validating timeseries lengths: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class EVModelEnergyDemand(Rule):
    """
    Validate total energy demand in model.

    Compares with approximate value: ev_count * 14000 km/a * 0.17 kWh/km.
    Allows 10% deviation.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str = "eGon2035",
        rtol: float = 0.1,
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
        """Evaluate total energy demand."""
        try:
            scenario_var_name = get_scenario_variation_name(self.scenario)

            # Get EV count allocated
            with db.session_scope() as session:
                query = session.query(
                    func.count(EgonEvMvGridDistrict.egon_ev_pool_ev_id).label(
                        "ev_count"
                    ),
                ).filter(
                    EgonEvMvGridDistrict.scenario == self.scenario,
                    EgonEvMvGridDistrict.scenario_variation
                    == scenario_var_name,
                )
            ev_count_alloc = (
                pd.read_sql(
                    query.statement, query.session.bind, index_col=None
                )
                .iloc[0]
                .ev_count
            )

            # Get load timeseries
            with db.session_scope() as session:
                subquery = (
                    session.query(EgonPfHvLoad.load_id)
                    .filter(EgonPfHvLoad.carrier == "land_transport_EV")
                    .filter(EgonPfHvLoad.scn_name == self.scenario)
                    .subquery()
                )
                query = session.query(
                    EgonPfHvLoadTimeseries.load_id,
                    EgonPfHvLoadTimeseries.p_set,
                ).filter(
                    EgonPfHvLoadTimeseries.load_id.in_(subquery),
                    EgonPfHvLoadTimeseries.scn_name == self.scenario,
                )
            load_ts = pd.read_sql(
                query.statement, query.session.bind, index_col="load_id"
            )

            total_energy_model = (
                load_ts.p_set.apply(lambda _: sum(_)).sum() / 1e6
            )
            total_energy_approx = ev_count_alloc * 14000 * 0.17 / 1e9

            rtol = self.params.get("rtol", 0.1)
            if np.allclose(total_energy_model, total_energy_approx, rtol=rtol):
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    observed=total_energy_model,
                    expected=total_energy_approx,
                    message=(
                        f"EV energy demand valid for {self.scenario}: "
                        f"{total_energy_model:.2f} TWh "
                        f"(approx: {total_energy_approx:.2f} TWh)"
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
                    observed=total_energy_model,
                    expected=total_energy_approx,
                    message=(
                        f"EV energy demand deviates >10% for "
                        f"{self.scenario}: {total_energy_model:.2f} vs "
                        f"{total_energy_approx:.2f} TWh"
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
                message=f"Error validating energy demand: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class EVModelStorageCapacity(Rule):
    """
    Validate total storage capacity against simBEV data.

    Allows 1% deviation.
    """

    def __init__(
        self,
        table: str,
        rule_id: str,
        scenario: str = "eGon2035",
        rtol: float = 0.01,
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
        """Evaluate storage capacity."""
        try:
            scenario_var_name = get_scenario_variation_name(self.scenario)
            meta_tech_data = read_simbev_metadata_file(
                self.scenario, "tech_data"
            )

            # Load storage capacity from model
            with db.session_scope() as session:
                query = session.query(
                    func.sum(EgonPfHvStore.e_nom).label("e_nom")
                ).filter(
                    EgonPfHvStore.scn_name == self.scenario,
                    EgonPfHvStore.carrier == "battery_storage",
                )
            storage_capacity_model = (
                pd.read_sql(
                    query.statement, query.session.bind, index_col=None
                ).e_nom.sum()
                / 1e3
            )

            # Load occurences of each EV
            with db.session_scope() as session:
                query = (
                    session.query(
                        EgonEvMvGridDistrict.bus_id,
                        EgonEvPool.type,
                        func.count(
                            EgonEvMvGridDistrict.egon_ev_pool_ev_id
                        ).label("count"),
                    )
                    .join(
                        EgonEvPool,
                        EgonEvPool.ev_id
                        == EgonEvMvGridDistrict.egon_ev_pool_ev_id,
                    )
                    .filter(
                        EgonEvMvGridDistrict.scenario == self.scenario,
                        EgonEvMvGridDistrict.scenario_variation
                        == scenario_var_name,
                        EgonEvPool.scenario == self.scenario,
                    )
                    .group_by(EgonEvMvGridDistrict.bus_id, EgonEvPool.type)
                )
            count_per_ev_all = pd.read_sql(
                query.statement, query.session.bind, index_col="bus_id"
            )
            count_per_ev_all["bat_cap"] = count_per_ev_all.type.map(
                meta_tech_data.battery_capacity
            )
            count_per_ev_all["bat_cap_total_MWh"] = (
                count_per_ev_all["count"] * count_per_ev_all.bat_cap / 1e3
            )
            storage_capacity_simbev = count_per_ev_all.bat_cap_total_MWh.div(
                1e3
            ).sum()

            rtol = self.params.get("rtol", 0.01)
            if np.allclose(
                storage_capacity_model, storage_capacity_simbev, rtol=rtol
            ):
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    observed=storage_capacity_model,
                    expected=storage_capacity_simbev,
                    message=(
                        f"EV storage capacity valid for {self.scenario}: "
                        f"{storage_capacity_model:.1f} GWh"
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
                    observed=storage_capacity_model,
                    expected=storage_capacity_simbev,
                    message=(
                        f"EV storage capacity deviates >1% for "
                        f"{self.scenario}: {storage_capacity_model:.1f} vs "
                        f"{storage_capacity_simbev:.1f} GWh"
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
                message=f"Error validating storage capacity: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class EVModelSoCConstraint(Rule):
    """
    Validate SoC storage constraint: e_min_pu < e_max_pu for all timesteps.
    """

    def __init__(
        self, table: str, rule_id: str, scenario: str = "eGon2035", **kwargs
    ):
        super().__init__(
            rule_id=rule_id, table=table, scenario=scenario, **kwargs
        )
        self.kind = "sanity"
        self.scenario = scenario

    def evaluate(self, engine, ctx):
        """Evaluate SoC constraints."""
        try:
            with db.session_scope() as session:
                subquery = (
                    session.query(EgonPfHvStore.store_id)
                    .filter(EgonPfHvStore.carrier == "battery_storage")
                    .filter(EgonPfHvStore.scn_name == self.scenario)
                    .subquery()
                )
                query = session.query(
                    EgonPfHvStoreTimeseries.store_id,
                    EgonPfHvStoreTimeseries.e_min_pu,
                    EgonPfHvStoreTimeseries.e_max_pu,
                ).filter(
                    EgonPfHvStoreTimeseries.store_id.in_(subquery),
                    EgonPfHvStoreTimeseries.scn_name == self.scenario,
                )
            store_ts = pd.read_sql(
                query.statement, query.session.bind, index_col="store_id"
            )

            stores_with_invalid_soc = []
            for idx, row in store_ts.iterrows():
                ts = row[["e_min_pu", "e_max_pu"]]
                x = np.array(ts.e_min_pu) > np.array(ts.e_max_pu)
                if x.any():
                    stores_with_invalid_soc.append(idx)

            if len(stores_with_invalid_soc) == 0:
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    observed=0,
                    expected=0,
                    message=f"All SoC constraints valid for {self.scenario}",
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
                    observed=len(stores_with_invalid_soc),
                    expected=0,
                    message=(
                        f"{len(stores_with_invalid_soc)} stores violate "
                        f"e_min_pu < e_max_pu for {self.scenario}: "
                        f"{stores_with_invalid_soc[:10]}..."
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
                message=f"Error validating SoC constraints: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )


class EVLowflexDrivingLoad(Rule):
    """
    Validate driving load vs charging load for lowflex scenario.

    Ratio should match charging efficiency (eta_cp).
    Only for eGon2035 vs eGon2035_lowflex comparison.
    """

    def __init__(self, table: str, rule_id: str, rtol: float = 0.01, **kwargs):
        super().__init__(rule_id=rule_id, table=table, rtol=rtol, **kwargs)
        self.kind = "sanity"

    def evaluate(self, engine, ctx):
        """Evaluate driving vs charging load."""
        try:
            meta_run_config = read_simbev_metadata_file(
                "eGon2035", "config"
            ).loc["basic"]

            # Load driving load (eGon2035)
            with db.session_scope() as session:
                query = (
                    session.query(
                        EgonPfHvLoad.load_id,
                        EgonPfHvLoadTimeseries.p_set,
                    )
                    .join(
                        EgonPfHvLoadTimeseries,
                        EgonPfHvLoadTimeseries.load_id == EgonPfHvLoad.load_id,
                    )
                    .filter(
                        EgonPfHvLoad.carrier == "land_transport_EV",
                        EgonPfHvLoad.scn_name == "eGon2035",
                        EgonPfHvLoadTimeseries.scn_name == "eGon2035",
                    )
                )
            model_driving_load = pd.read_sql(
                query.statement, query.session.bind, index_col=None
            )
            driving_load = np.array(model_driving_load.p_set.to_list()).sum(
                axis=0
            )

            # Load charging load (eGon2035_lowflex)
            with db.session_scope() as session:
                query = (
                    session.query(
                        EgonPfHvLoad.load_id,
                        EgonPfHvLoadTimeseries.p_set,
                    )
                    .join(
                        EgonPfHvLoadTimeseries,
                        EgonPfHvLoadTimeseries.load_id == EgonPfHvLoad.load_id,
                    )
                    .filter(
                        EgonPfHvLoad.carrier == "land_transport_EV",
                        EgonPfHvLoad.scn_name == "eGon2035_lowflex",
                        EgonPfHvLoadTimeseries.scn_name == "eGon2035_lowflex",
                    )
                )
            model_charging_load_lowflex = pd.read_sql(
                query.statement, query.session.bind, index_col=None
            )
            charging_load = np.array(
                model_charging_load_lowflex.p_set.to_list()
            ).sum(axis=0)

            driving_load_theoretical = (
                float(meta_run_config.eta_cp) * charging_load.sum()
            )

            rtol = self.params.get("rtol", 0.01)
            if np.allclose(
                driving_load.sum(), driving_load_theoretical, rtol=rtol
            ):
                return RuleResult(
                    rule_id=self.rule_id,
                    task=self.task,
                    table=self.table,
                    kind=self.kind,
                    success=True,
                    observed=driving_load.sum() / 1e6,
                    expected=driving_load_theoretical / 1e6,
                    message=(
                        f"Driving load matches theoretical: "
                        f"{driving_load.sum() / 1e6:.2f} TWh"
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
                    observed=driving_load.sum() / 1e6,
                    expected=driving_load_theoretical / 1e6,
                    message=(
                        f"Driving load deviates >1% from theoretical: "
                        f"{driving_load.sum() / 1e6:.2f} vs "
                        f"{driving_load_theoretical / 1e6:.2f} TWh"
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
                message=f"Error validating lowflex driving load: {str(e)}",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__,
            )

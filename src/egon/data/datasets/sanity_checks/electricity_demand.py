"""Consistency (sum) checks for the ``electricity_demand`` TaskGroup.

This module is meant to run as the very last task of the
``electricity_demand`` TaskGroup (see
:py:mod:`egon.data.airflow.dags.pipeline`). It does not create or
modify any data. For every scenario configured via ``--scenarios``, it
checks that annual electricity demand is conserved through each step of
the TaskGroup's processing chain:

    published NEP target -> DemandRegio (NUTS-3) -> census cells /
    disaggregated curves -> final AC load exported to eTraGo

as well as one check that isn't scenario-dependent: that the
census-based household-type refinement performed by ``hh_profiles``
conserves the original census counts.

Only tables filled by tasks of this TaskGroup are read, with two
exceptions used purely to scope/group those results correctly, never to
validate their own content:

- ``grid.egon_etrago_bus`` (``country='DE'``): ``electrical_load_etrago``
  only ever writes rows for German buses into the *shared* tables
  ``grid.egon_etrago_load``/``..._load_timeseries`` (other TaskGroups,
  e.g. ``electrical_neighbours``, write AC loads for foreign buses into
  the very same tables). Joining on this table with the same filter the
  writing task itself uses is the only way to identify "this
  TaskGroup's own rows" in that shared table.
- ``boundaries.egon_map_zensus_vg250``: a static census-cell-to-NUTS3
  mapping, needed to aggregate the census-cell-level demand table
  ``egon_demandregio_zensus_electricity`` up to the NUTS-3 level of
  ``egon_demandregio_hh``/``egon_demandregio_cts_ind`` for comparison.

Deviations are reported via ``logger.warning`` instead of raising an
exception, so a single inconsistency does not block the whole pipeline
run.
"""

from egon.data import config, db
from egon.data.datasets import Dataset, DatasetSources
from egon.data.datasets.sanity_checks import (
    REL_TOLERANCE,
    evaluate,
    evaluate_grouped,
    log_table,
    sql_sum,
)
from egon.data.datasets.scenario_parameters import get_sector_parameters

TAG = "electricity_demand"

# The refined/census household counts are expected to match almost
# exactly (same source data, just re-aggregated), so this check uses a
# much tighter tolerance than the cross-table demand comparisons.
HH_REFINEMENT_RTOL = 1e-3


def _demand_regio_totals(scenario):
    """National totals of DemandRegio's own (scaled) output, per
    sector, for `scenario`."""

    return {
        "households": sql_sum(
            "demand.egon_demandregio_hh", f"scenario = '{scenario}'"
        ),
        "CTS": sql_sum(
            "demand.egon_demandregio_cts_ind",
            f"scenario = '{scenario}' AND wz IN "
            "(SELECT wz FROM demand.egon_demandregio_wz "
            "WHERE sector = 'CTS')",
        ),
        "industry": sql_sum(
            "demand.egon_demandregio_cts_ind",
            f"scenario = '{scenario}' AND wz IN "
            "(SELECT wz FROM demand.egon_demandregio_wz "
            "WHERE sector = 'industry')",
        ),
    }


def _annual_demand_target_rows(scenario, demandregio):
    """DemandRegio's own totals vs. the published targets they are
    scaled to (`demandregio.insert_hh_demand`/`insert_cts_ind`, scaled
    from `get_sector_parameters("electricity", scenario)
    ["annual_demand"]`, sourced from NEP figures in
    `scenario_parameters/parameters.py`). The one genuinely external
    reference value used anywhere in this TaskGroup; every other check
    here only verifies internal consistency between tables that both
    derive from DemandRegio's own output."""

    targets = get_sector_parameters("electricity", scenario=scenario)[
        "annual_demand"
    ]

    return [
        evaluate(
            "HH target",
            demandregio["households"],
            float(targets["households"]),
        ),
        evaluate("CTS target", demandregio["CTS"], float(targets["CTS"])),
        evaluate(
            "Ind target", demandregio["industry"], float(targets["industry"])
        ),
    ]


def _demand_conservation_rows(scenario, demandregio):
    """Demand conservation from DemandRegio's NUTS-3 totals down to the
    final AC load exported to eTraGo."""

    rows = []

    # Household demand: census cells vs. DemandRegio, per NUTS-3 region
    hh_df = db.select_dataframe(
        f"""
        SELECT dr.nuts3, dr.demand_regio_sum AS expected,
               profiles.profile_sum AS actual
        FROM (
            SELECT vg250_nuts3 AS nuts3, SUM(demand) AS profile_sum
            FROM demand.egon_demandregio_zensus_electricity egon
            JOIN boundaries.egon_map_zensus_vg250 boundaries
                ON egon.zensus_population_id
                    = boundaries.zensus_population_id
            WHERE scenario = '{scenario}' AND sector = 'residential'
            GROUP BY vg250_nuts3
        ) profiles
        JOIN (
            SELECT nuts3, SUM(demand) AS demand_regio_sum
            FROM demand.egon_demandregio_hh
            WHERE scenario = '{scenario}'
            GROUP BY nuts3
        ) dr ON profiles.nuts3 = dr.nuts3
        """,
        warning=False,
    )
    row, _, _ = evaluate_grouped("HH demand", hh_df)
    rows.append(row)

    # CTS demand: census cells vs. DemandRegio, per NUTS-3 region
    cts_df = db.select_dataframe(
        f"""
        SELECT dr.nuts3, dr.demand_regio_sum AS expected,
               profiles.profile_sum AS actual
        FROM (
            SELECT vg250_nuts3 AS nuts3, SUM(demand) AS profile_sum
            FROM demand.egon_demandregio_zensus_electricity egon
            JOIN boundaries.egon_map_zensus_vg250 boundaries
                ON egon.zensus_population_id
                    = boundaries.zensus_population_id
            WHERE scenario = '{scenario}' AND sector = 'service'
            GROUP BY vg250_nuts3
        ) profiles
        JOIN (
            SELECT nuts3, SUM(demand) AS demand_regio_sum
            FROM demand.egon_demandregio_cts_ind
            WHERE scenario = '{scenario}'
            AND wz IN (
                SELECT wz FROM demand.egon_demandregio_wz
                WHERE sector = 'CTS')
            GROUP BY nuts3
        ) dr ON profiles.nuts3 = dr.nuts3
        """,
        warning=False,
    )
    row, _, _ = evaluate_grouped("CTS demand", cts_df)
    rows.append(row)

    # Industrial demand: OSM landuse areas + industrial sites vs.
    # DemandRegio, national total
    industry_actual = sql_sum(
        "demand.egon_demandregio_osm_ind_electricity",
        f"scenario = '{scenario}'",
    ) + sql_sum(
        "demand.egon_demandregio_sites_ind_electricity",
        f"scenario = '{scenario}'",
    )
    rows.append(
        evaluate("Ind demand", industry_actual, demandregio["industry"])
    )

    # Final AC load exported to eTraGo (household + CTS + industry) vs.
    # DemandRegio totals, national total
    final_load = db.select_dataframe(
        f"""
        SELECT SUM(p) AS s FROM (
            SELECT UNNEST(b.p_set) AS p
            FROM grid.egon_etrago_load a
            JOIN grid.egon_etrago_load_timeseries b
                ON a.scn_name = b.scn_name AND a.load_id = b.load_id
            JOIN grid.egon_etrago_bus c
                ON a.bus = c.bus_id AND a.scn_name = c.scn_name
            WHERE a.scn_name = '{scenario}'
                AND a.carrier = 'AC'
                AND c.country = 'DE'
        ) t
        """,
        warning=False,
    )["s"][0]
    final_load = 0.0 if final_load is None else float(final_load)
    rows.append(
        evaluate(
            "Final load",
            final_load,
            sum(demandregio.values()),
        )
    )

    return rows


def _household_refinement_rows():
    """Refined 10-type household counts vs. the original 5-type census
    counts they were derived from, per NUTS-3 and household
    characteristic. Not scenario-dependent, checked once."""

    df = db.select_dataframe(
        """
        SELECT refined.nuts3 || '-' || refined.characteristics_code
            AS nuts3,
            census.sum_census AS expected, refined.sum_refined AS actual
        FROM (
            SELECT nuts3, characteristics_code, SUM(hh_10types)
                AS sum_refined
            FROM society.egon_destatis_zensus_household_per_ha_refined
            GROUP BY nuts3, characteristics_code
        ) refined
        JOIN (
            SELECT t.nuts3, t.characteristics_code, SUM(orig)
                AS sum_census
            FROM (
                SELECT nuts3, cell_id, characteristics_code,
                    SUM(DISTINCT(hh_5types)) AS orig
                FROM society.egon_destatis_zensus_household_per_ha_refined
                GROUP BY cell_id, characteristics_code, nuts3
            ) t
            GROUP BY t.nuts3, t.characteristics_code
        ) census
        ON refined.nuts3 = census.nuts3
        AND refined.characteristics_code = census.characteristics_code
        """,
        warning=False,
    )
    row, _, _ = evaluate_grouped("HH refine", df, rtol=HH_REFINEMENT_RTOL)
    return [row]


def sanity_checks():
    """Consistency checks for the ``electricity_demand`` TaskGroup.

    Logs one table per scenario (DemandRegio scaling targets + demand
    conservation down to the final eTraGo load), plus one table for the
    scenario-independent household-type refinement check. Never raises;
    problems are reported as warnings.
    """

    scenarios = config.settings()["egon-data"]["--scenarios"]

    log_table(
        TAG,
        "Household-type refinement, worst of the NUTS-3 x household-"
        f"characteristic groups (tolerance {HH_REFINEMENT_RTOL * 100:.1f} %)",
        _household_refinement_rows(),
    )

    for scenario in scenarios:
        demandregio = _demand_regio_totals(scenario)
        rows = _annual_demand_target_rows(
            scenario, demandregio
        ) + _demand_conservation_rows(scenario, demandregio)
        log_table(
            TAG,
            f"scenario '{scenario}': DemandRegio scaling targets and "
            f"demand conservation (tolerance {REL_TOLERANCE * 100:.1f} %)",
            rows,
        )


class ElectricityDemandSanityCheck(Dataset):
    """
    Sanity check for the whole ``electricity_demand`` TaskGroup.

    Runs as the last task of the TaskGroup and checks, for every
    configured scenario, that annual electricity demand is conserved
    through each processing step, from the published NEP target down to
    the final AC load exported to eTraGo. Deviations are reported via
    log warnings; this dataset never fails the pipeline since its
    purpose is to surface problems, not to block the DAG.

    *Dependencies*
      * All datasets of the ``electricity_demand`` TaskGroup

    *Resulting tables*
      * None - this dataset only reads and logs, it writes nothing.
    """

    #:
    name: str = "ElectricityDemandSanityCheck"
    #:
    version: str = "0.0.1"

    sources = DatasetSources(
        tables={
            "demandregio_hh": "demand.egon_demandregio_hh",
            "demandregio_cts_ind": "demand.egon_demandregio_cts_ind",
            "demandregio_wz": "demand.egon_demandregio_wz",
            "zensus_electricity": "demand.egon_demandregio_zensus_electricity",
            "osm_ind_electricity": "demand.egon_demandregio_osm_ind_electricity",
            "sites_ind_electricity": "demand.egon_demandregio_sites_ind_electricity",
            "etrago_load": "grid.egon_etrago_load",
            "etrago_load_curves": "grid.egon_etrago_load_timeseries",
            "hh_refinement": "society.egon_destatis_zensus_household_per_ha_refined",  # noqa: E501
            # Not filled by this TaskGroup - used only to scope/group
            # the tables above correctly, see the module docstring.
            "etrago_bus": "grid.egon_etrago_bus",
            "map_zensus_vg250": "boundaries.egon_map_zensus_vg250",
        }
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(sanity_checks,),
        )

"""
Sanity check validation rules for the heat demand per census cell.

``demand.egon_peta_heat`` holds the residential and service-sector heat
demand per census cell for every configured scenario. All scenarios are the
same Peta raster scaled by a scenario-specific factor, which these rules make
use of: each scenario has to cover the same census cells.
"""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity


def _result(rule, success, observed, expected, message):
    """RuleResult with the fields shared by all rules in this module."""
    return RuleResult(
        rule_id=rule.rule_id,
        task=rule.task,
        table=rule.table,
        kind=rule.kind,
        success=success,
        observed=observed,
        expected=expected,
        message=message,
        severity=Severity.INFO if success else Severity.ERROR,
        schema=rule.schema,
        table_name=rule.table_name,
        rule_class=rule.__class__.__name__,
    )


class HeatDemandUniqueCells(DataFrameRule):
    """
    Validate that each census cell appears once per scenario and sector.

    Downstream datasets sum the demand per census cell, so a cell listed
    twice for the same scenario and sector would be counted twice.
    """

    def __init__(self, table: str, rule_id: str, **kwargs):
        """
        Parameters
        ----------
        table : str
            Target table ("demand.egon_peta_heat").
        rule_id : str
            Unique identifier for this validation rule.
        """
        super().__init__(rule_id=rule_id, table=table, **kwargs)
        self.kind = "sanity"

    def get_query(self, ctx):
        """Number of duplicated cells and of the rows they occupy."""
        return f"""
        SELECT
            count(*) AS n_duplicated_cells,
            coalesce(sum(n_rows), 0) AS n_rows
        FROM (
            SELECT count(*) AS n_rows
            FROM {self.table}
            GROUP BY scenario, sector, zensus_population_id
            HAVING count(*) > 1
        ) AS duplicates
        """

    def evaluate_df(self, df, ctx):
        """Fail if any cell appears more than once."""
        n_duplicated_cells = int(df["n_duplicated_cells"].values[0] or 0)
        n_rows = int(df["n_rows"].values[0] or 0)
        success = n_duplicated_cells == 0

        if success:
            message = (
                "Every census cell appears once per scenario and sector"
            )
        else:
            message = (
                f"{n_duplicated_cells} census cells appear more than once "
                f"per scenario and sector ({n_rows} rows)"
            )

        return _result(self, success, n_duplicated_cells, 0, message)


class HeatDemandScenarioCellConsistency(DataFrameRule):
    """
    Validate that all scenarios cover the same census cells.

    Each scenario is the same Peta raster scaled by a factor, so for each
    sector every census cell must appear in either all scenarios or none.
    This works for any number of configured scenarios; with a single one,
    it always passes.
    """

    def __init__(self, table: str, rule_id: str, **kwargs):
        """
        Parameters
        ----------
        table : str
            Target table ("demand.egon_peta_heat").
        rule_id : str
            Unique identifier for this validation rule.
        """
        super().__init__(rule_id=rule_id, table=table, **kwargs)
        self.kind = "sanity"

    def get_query(self, ctx):
        """Per sector: number of scenarios, cells, and cells not in all."""
        return f"""
        WITH scenarios AS (
            SELECT sector, count(DISTINCT scenario) AS n_scenarios
            FROM {self.table}
            GROUP BY sector
        ),
        cells AS (
            SELECT
                sector,
                zensus_population_id,
                count(DISTINCT scenario) AS n_scenarios
            FROM {self.table}
            GROUP BY sector, zensus_population_id
        )
        SELECT
            cells.sector,
            scenarios.n_scenarios,
            count(*) AS n_cells,
            count(*) FILTER (
                WHERE cells.n_scenarios < scenarios.n_scenarios
            ) AS n_incomplete_cells
        FROM cells
        JOIN scenarios ON scenarios.sector = cells.sector
        GROUP BY cells.sector, scenarios.n_scenarios
        ORDER BY cells.sector
        """

    def evaluate_df(self, df, ctx):
        """Fail if any cell is missing from at least one scenario."""
        n_incomplete_cells = int(df["n_incomplete_cells"].sum())
        success = n_incomplete_cells == 0

        details = "; ".join(
            f"{row.sector}: {row.n_cells} cells in {row.n_scenarios} "
            f"scenarios, {row.n_incomplete_cells} not in all of them"
            for row in df.itertuples()
        )
        message = (
            f"All scenarios cover the same cells. {details}"
            if success
            else f"{n_incomplete_cells} cells are missing from at least one "
            f"scenario. {details}"
        )

        return _result(self, success, n_incomplete_cells, 0, message)

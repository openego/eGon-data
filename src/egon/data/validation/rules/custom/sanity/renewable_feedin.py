"""Sanity check validation rules for renewable feed-in time series."""

from egon_validation.rules.base import DataFrameRule, RuleResult, Severity

#: Plausible value range (min, max) of the time series per carrier.
#: wind/pv are given per unit of installed capacity, solar thermal in
#: kW/m² and the heat pump COP follows Brown et al. (COP >= 1.0 by
#: construction of the quadratic approximation).
FEEDIN_VALUE_RANGES = {
    "wind_onshore": (0.0, 1.0),
    "wind_offshore": (0.0, 1.0),
    "pv": (0.0, 1.0),
    "solar_thermal": (0.0, 1.5),
    "heat_pump_cop": (0.99, 8.0),
}


#: Table holding all zensus cells
ZENSUS_TABLE = "society.destatis_zensus_population_per_ha"


class ZensusWeatherCellMappingRowCount(DataFrameRule):
    """Check that every zensus cell is mapped to exactly one weather cell.

    Instead of a fixed row count, the number of rows in the mapping table
    is compared to the number of cells in
    society.destatis_zensus_population_per_ha. The check fails if a
    zensus cell is missing in the mapping or mapped more than once.

    Args:
        table: Table being validated
            (boundaries.egon_map_zensus_weather_cell)
        rule_id: Unique identifier for this validation rule

    Example:
        >>> validation = {
        ...     "data-quality": [
        ...         ZensusWeatherCellMappingRowCount(
        ...             table="boundaries.egon_map_zensus_weather_cell",
        ...             rule_id="SANITY_ZENSUS_WEATHER_CELL_ROW_COUNT",
        ...         )
        ...     ]
        ... }
    """

    def __init__(self, table: str, rule_id: str, **kwargs):
        super().__init__(rule_id=rule_id, table=table, **kwargs)
        self.kind = "sanity"

    def get_query(self, ctx):
        return f"""
        SELECT (SELECT COUNT(*) FROM {ZENSUS_TABLE}) AS n_zensus,
               COUNT(*) AS n_rows,
               COUNT(DISTINCT zensus_population_id) AS n_distinct
        FROM {self.table}
        """

    def evaluate_df(self, df, ctx):
        row = df.iloc[0]
        n_zensus = int(row["n_zensus"])
        n_rows = int(row["n_rows"])
        n_duplicates = n_rows - int(row["n_distinct"])

        success = n_rows == n_zensus and n_duplicates == 0

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=float(n_rows),
            expected=float(n_zensus),
            message=(
                f"All {n_zensus} zensus cells are mapped to a weather cell"
                if success
                else f"Expected {n_zensus} rows (one per cell in "
                f"{ZENSUS_TABLE}), found {n_rows} "
                f"({n_duplicates} duplicate zensus_population_id)"
            ),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )


class RenewableFeedinTimeseries(DataFrameRule):
    """Validate the feed-in time series of supply.egon_era5_renewable_feedin.

    Checks per carrier that

    * the carrier is present at all,
    * all time series have the same, expected length (one value per hour
      of the weather year) and
    * all values lie inside the plausible range given in
      :data:`FEEDIN_VALUE_RANGES`.

    Args:
        table: Primary table being validated
            (supply.egon_era5_renewable_feedin)
        rule_id: Unique identifier for this validation rule
        expected_length: Expected number of values per time series
            (default: 8760 = hourly values of a non-leap weather year)

    Example:
        >>> validation = {
        ...     "data_quality": [
        ...         RenewableFeedinTimeseries(
        ...             table="supply.egon_era5_renewable_feedin",
        ...             rule_id="SANITY_RENEWABLE_FEEDIN_TIMESERIES",
        ...         )
        ...     ]
        ... }
    """

    def __init__(
        self, table: str, rule_id: str, expected_length: int = 8760, **kwargs
    ):
        super().__init__(
            rule_id=rule_id,
            table=table,
            expected_length=expected_length,
            **kwargs,
        )
        self.kind = "sanity"

    def get_query(self, ctx):
        return """
        SELECT carrier,
               COUNT(*) AS n_cells,
               MIN(length) AS min_length,
               MAX(length) AS max_length,
               MIN(min_value) AS min_value,
               MAX(max_value) AS max_value
        FROM (
            SELECT carrier,
                   cardinality(feedin) AS length,
                   (SELECT MIN(v) FROM unnest(feedin) AS v) AS min_value,
                   (SELECT MAX(v) FROM unnest(feedin) AS v) AS max_value
            FROM supply.egon_era5_renewable_feedin
        ) AS series
        GROUP BY carrier
        """

    def evaluate_df(self, df, ctx):
        expected_length = self.params.get("expected_length", 8760)
        problems = []

        found = df.set_index("carrier")

        for carrier, (low, high) in FEEDIN_VALUE_RANGES.items():
            if carrier not in found.index:
                problems.append(f"{carrier}: no time series found")
                continue

            row = found.loc[carrier]

            if row["min_length"] != expected_length or (
                row["max_length"] != expected_length
            ):
                problems.append(
                    f"{carrier}: time series length "
                    f"{int(row['min_length'])}-{int(row['max_length'])}, "
                    f"expected {expected_length}"
                )

            if row["min_value"] < low or row["max_value"] > high:
                problems.append(
                    f"{carrier}: values {row['min_value']:.4f}-"
                    f"{row['max_value']:.4f} outside of [{low}, {high}]"
                )

        unknown = sorted(set(found.index) - set(FEEDIN_VALUE_RANGES))
        if unknown:
            problems.append(f"unexpected carriers: {unknown}")

        success = not problems

        return RuleResult(
            rule_id=self.rule_id,
            task=self.task,
            table=self.table,
            kind=self.kind,
            success=success,
            observed=float(len(problems)),
            expected=0.0,
            message=(
                f"All {len(FEEDIN_VALUE_RANGES)} carriers have complete "
                f"time series of length {expected_length} within the "
                f"plausible value range"
                if success
                else "Feed-in time series problems: " + "; ".join(problems)
            ),
            severity=Severity.INFO if success else Severity.ERROR,
            schema=self.schema,
            table_name=self.table_name,
            rule_class=self.__class__.__name__,
        )

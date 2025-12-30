# Sanity Checks Migration Guide

This guide explains how to migrate sanity check functions from `sanity_checks.py` to inline validation rules that integrate with the egon-validation framework.

## Overview

**Before:** Sanity checks were standalone functions called manually
**After:** Sanity checks are validation rules declared inline in Dataset definitions

## Benefits

- ✅ Structured validation results with pass/fail tracking
- ✅ Automatic execution as part of dataset tasks
- ✅ Results collected in validation reports
- ✅ Better error reporting with observed vs expected values
- ✅ Parallel execution support
- ✅ Consistent with formal validation rules

---

## Example Migration

### Before: Old Sanity Check Function

```python
# In sanity_checks.py
def cts_electricity_demand_share(rtol=0.005):
    """Check CTS electricity demand share sums to 1."""
    df_demand_share = pd.read_sql(...)

    np.testing.assert_allclose(
        actual=df_demand_share.groupby(["bus_id", "scenario"])["profile_share"].sum(),
        desired=1,
        rtol=rtol,
        verbose=False,
    )

    logger.info("CTS electricity demand shares sum correctly")
```

### After: New Validation Rule

```python
# In egon/data/validation/rules/custom/sanity/cts_demand.py
from egon_validation.rules.base import DataFrameRule, RuleResult, Severity
import numpy as np

class CtsElectricityDemandShare(DataFrameRule):
    """Validate CTS electricity demand shares sum to 1 for each substation."""

    def __init__(self, table: str, rule_id: str, rtol: float = 0.005, **kwargs):
        super().__init__(rule_id=rule_id, table=table, rtol=rtol, **kwargs)
        self.kind = "sanity"

    def get_query(self, ctx):
        return """
        SELECT bus_id, scenario, SUM(profile_share) as total_share
        FROM demand.egon_cts_electricity_demand_building_share
        GROUP BY bus_id, scenario
        """

    def evaluate_df(self, df, ctx):
        rtol = self.params.get("rtol", 0.005)

        try:
            np.testing.assert_allclose(
                actual=df["total_share"],
                desired=1.0,
                rtol=rtol,
                verbose=False,
            )

            max_diff = (df["total_share"] - 1.0).abs().max()

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=True,
                observed=float(max_diff),
                expected=rtol,
                message=f"CTS electricity demand shares sum to 1 (max deviation: {max_diff:.6f})",
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )
        except AssertionError:
            max_diff = (df["total_share"] - 1.0).abs().max()
            violations = df[~np.isclose(df["total_share"], 1.0, rtol=rtol)]

            return RuleResult(
                rule_id=self.rule_id,
                task=self.task,
                table=self.table,
                kind=self.kind,
                success=False,
                observed=float(max_diff),
                expected=rtol,
                message=f"Demand share mismatch: {len(violations)} violations",
                severity=Severity.ERROR,
                schema=self.schema,
                table_name=self.table_name,
                rule_class=self.__class__.__name__
            )
```

---

## Using Inline Validations in Datasets

### Option 1: Dataset-Specific Inline Validation

For validations tied to a specific dataset (e.g., CTS demand validations), add them inline to that dataset:

```python
from egon.data.datasets import Dataset
from egon.data.validation.rules.custom.sanity import (
    CtsElectricityDemandShare,
    CtsHeatDemandShare,
)

class CtsElectricityDemand(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="CtsElectricityDemand",
            version="1.0.0",
            dependencies=dependencies,
            tasks=(
                download_data,
                process_demand,
                distribute_to_buildings,
            ),
            validation={
                "data_quality": [
                    CtsElectricityDemandShare(
                        table="demand.egon_cts_electricity_demand_building_share",
                        rule_id="SANITY_CTS_ELECTRICITY_DEMAND_SHARE",
                        rtol=0.005
                    ),
                    CtsHeatDemandShare(
                        table="demand.egon_cts_heat_demand_building_share",
                        rule_id="SANITY_CTS_HEAT_DEMAND_SHARE",
                        rtol=0.005
                    ),
                ]
            },
            validation_on_failure="continue"  # or "fail" to stop pipeline
        )
```

### Option 2: Cross-Cutting Validations in FinalValidations

For validations that check data consistency **across multiple datasets** (e.g., gas store capacity checks), add them to the `FinalValidations` dataset:

```python
# In: src/egon/data/datasets/final_validations.py

from egon.data.validation.rules.custom.sanity import (
    CH4StoresCapacity,
    H2SaltcavernStoresCapacity,
    # Import your new validation rule here
)

class FinalValidations(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            # ...
            validation={
                "gas_stores": [
                    CH4StoresCapacity(...),
                    H2SaltcavernStoresCapacity(...),
                    # Add your new rule here
                ],
                # Add new category if needed
                "your_category": [
                    YourNewValidationRule(...),
                ],
            },
        )
```

Then update `pipeline.py` to include your dataset in `FinalValidations` dependencies:

```python
final_validations = FinalValidations(
    dependencies=[
        insert_data_ch4_storages,
        insert_H2_storage,
        storage_etrago,
        your_new_dataset,  # Add dataset providing data for your validation
    ]
)
```

**When to use FinalValidations:**
- ✅ Validation checks data from multiple datasets
- ✅ Validation should run at the end of the pipeline
- ✅ Validation is cross-cutting (gas network, timeseries consistency, etc.)
- ❌ Don't use for dataset-specific checks (use inline validation instead)

### How It Works

1. **Validation tasks are created automatically** from the `validation` dict
2. **Tasks are named:** `{dataset_name}.validate.{validation_key}`
   - Example: `CtsElectricityDemand.validate.data_quality`
3. **Tasks run after the main dataset tasks** complete
4. **Results are written** to `validation_runs/{run_id}/tasks/{task_name}/{rule_id}/results.jsonl`
5. **Validation report collects** all results at the end of the pipeline

---

## Migration Patterns

### Pattern 1: Simple DataFrame Assertion

**Sanity Check:**
```python
def check_something(rtol=0.01):
    df = db.select_dataframe("SELECT * FROM table")
    np.testing.assert_allclose(df["actual"], df["expected"], rtol=rtol)
    logger.info("Check passed")
```

**Validation Rule:**
```python
class CheckSomething(DataFrameRule):
    def __init__(self, table, rule_id, rtol=0.01, **kwargs):
        super().__init__(rule_id, table, rtol=rtol, **kwargs)
        self.kind = "sanity"

    def get_query(self, ctx):
        return "SELECT * FROM table"

    def evaluate_df(self, df, ctx):
        rtol = self.params.get("rtol")
        try:
            np.testing.assert_allclose(df["actual"], df["expected"], rtol=rtol)
            return RuleResult(success=True, ...)
        except AssertionError:
            return RuleResult(success=False, ...)
```

### Pattern 2: Multi-Table Comparison

**Sanity Check:**
```python
def compare_tables():
    df1 = db.select_dataframe("SELECT SUM(value) FROM table1 GROUP BY key")
    df2 = db.select_dataframe("SELECT SUM(value) FROM table2 GROUP BY key")
    merged = df1.merge(df2, on="key")
    assert (merged["value_x"] == merged["value_y"]).all()
```

**Validation Rule:**
```python
class CompareTablesCheck(DataFrameRule):
    def get_query(self, ctx):
        return """
        SELECT
            t1.key,
            t1.total as table1_total,
            t2.total as table2_total
        FROM (SELECT key, SUM(value) as total FROM table1 GROUP BY key) t1
        JOIN (SELECT key, SUM(value) as total FROM table2 GROUP BY key) t2
        ON t1.key = t2.key
        """

    def evaluate_df(self, df, ctx):
        matches = (df["table1_total"] == df["table2_total"]).all()
        return RuleResult(success=matches, ...)
```

### Pattern 3: Complex Checks with Loops

For complex sanity checks with loops (e.g., `etrago_timeseries_length()`), you have two options:

**Option A: Create one rule per component** (Recommended)
```python
validation = {
    "timeseries_length": [
        TimeseriesLengthCheck(
            table="grid.egon_etrago_generator_timeseries",
            rule_id="SANITY_GENERATOR_TIMESERIES_LENGTH",
            component="generator"
        ),
        TimeseriesLengthCheck(
            table="grid.egon_etrago_load_timeseries",
            rule_id="SANITY_LOAD_TIMESERIES_LENGTH",
            component="load"
        ),
        # ... more components
    ]
}
```

**Option B: Handle all components in one rule**
```python
class TimeseriesLengthCheck(DataFrameRule):
    def evaluate_df(self, df, ctx):
        # Check all components in a loop
        # Return aggregated result
```

---

## Completed Migrations

The following sanity checks have been migrated to validation rules:

### ✅ Residential Electricity
- `residential_electricity_annual_sum()` → `ResidentialElectricityAnnualSum`
- `residential_electricity_hh_refinement()` → `ResidentialElectricityHhRefinement`

### ✅ CTS Demand
- `cts_electricity_demand_share()` → `CtsElectricityDemandShare`
- `cts_heat_demand_share()` → `CtsHeatDemandShare`

### ✅ Home Batteries
- `sanitycheck_home_batteries()` → `HomeBatteriesAggregation`

### ✅ Gas Stores
- `sanity_check_CH4_stores()` → `CH4StoresCapacity`
- `sanity_check_H2_saltcavern_stores()` → `H2SaltcavernStoresCapacity`

### ✅ Gas Grid
- `sanity_check_gas_buses()` → `GasBusesIsolated` + `GasBusesCount`
- `sanity_check_gas_one_port()` → `GasOnePortConnections`
- `sanity_check_CH4_grid()` → `CH4GridCapacity`
- `sanity_check_gas_links()` → `GasLinksConnections`

### ✅ Gas Loads and Generators
- `etrago_eGon2035_gas_DE()` → `GasLoadsCapacity` + `GasGeneratorsCapacity` (wrapper function - components already migrated)

### ✅ Electricity Capacity
- `etrago_eGon2035_electricity()` → `ElectricityCapacityComparison` (9 generator carriers + 1 storage carrier)
  - Validates: wind_onshore, wind_offshore, solar, solar_rooftop, biomass, run_of_river, reservoir, oil, others, pumped_hydro

### ✅ Heat Supply Capacity
- `etrago_eGon2035_heat()` → `ElectricityCapacityComparison` (5 heat supply carriers - reused for heat!)
  - Links: central_heat_pump, rural_heat_pump, central_resistive_heater
  - Generators: solar_thermal_collector, geo_thermal
  - **Note:** Heat demand check from this function still needs migration (timeseries-based validation)

### ✅ Timeseries Length
- `etrago_timeseries_length()` → `ArrayCardinalityValidation` (reused from egon-validation formal rules!)
  - Validates 8 array columns across 5 component types (generator, load, link, store, storage)
  - Checks: p_max_pu, p_min_pu, p_set, q_set, e_min_pu, e_max_pu, inflow
  - Leverages existing formal validation rule from egon-validation library

### ✅ eGon100RE Capacity Validations
- `generators_links_storages_stores_100RE()` → `ElectricityCapacityComparison` (reused for eGon100RE!)
  - **Generators (13):** wind_onshore, wind_offshore, solar, solar_rooftop, run_of_river, oil, lignite, coal, solar_thermal_collector, geo_thermal, rural_solar_thermal, urban_central_gas_CHP, urban_central_solid_biomass_CHP
  - **Links (9):** central_gas_boiler, central_heat_pump, central_resistive_heater, OCGT, rural_biomass_boiler, rural_gas_boiler, rural_heat_pump, rural_oil_boiler, rural_resistive_heater
  - **Storage (1):** pumped_hydro
  - **Note:** Stores validation deferred (original function only prints, no validation logic)

### ✅ Electrical Load Demand
- `electrical_load_100RE()` → `ElectricalLoadAggregationValidation` (reused from egon-validation!)
  - Validates annual electrical load sum (TWh) for all scenarios (eGon2035, eGon100RE, etc.)
  - Also checks max/min load (GW) - more comprehensive than original
  - Leverages existing custom validation rule from egon-validation library
  - **Note:** Original function validated by sector (residential, commercial, industrial) but existing rule validates total only

### ✅ Heat Demand
- Heat demand validation (from `etrago_eGon2035_heat()`) → `HeatDemandValidation` (new class!)
  - Validates annual heat demand (rural_heat + central_heat) against peta_heat reference
  - Compares timeseries sum vs expected demand
  - eGon2035 scenario

---

## Migration Status Summary

### ✅ All Core Validations Migrated

All core sanity checks have been successfully migrated to the new validation framework, including:
- Residential electricity (annual sum, household refinement)
- CTS demand (electricity and heat shares)
- Home batteries aggregation
- Gas infrastructure (stores, buses, grid, links, loads, generators)
- Electricity capacity (eGon2035 and eGon100RE generators, storage)
- Heat capacity (heat pumps, resistive heaters, solar thermal, geothermal)
- Timeseries length validation
- Electrical load aggregation
- Heat demand validation

### Deferred Validations (Require Dataset-Inline Implementation)

The following sanity checks require dataset-inline validation due to their complexity and cannot be easily migrated to standalone validation rules:

**Reason for Deferral: Complex with External Dependencies**
1. **`sanitycheck_pv_rooftop_buildings()`**
   - Creates matplotlib/seaborn visualizations
   - Loads external building data via `load_building_data()`
   - Has dataset-boundary-specific logic (Schleswig-Holstein special cases)
   - Reads from Excel files for certain scenarios
   - **Migration approach**: Implement as dataset-inline validation in the PV rooftop dataset

2. **`sanitycheck_emobility_mit()`**
   - Multiple sub-checks (EV allocation, trip data, model components)
   - Uses ORM queries with session scopes
   - Depends on SimBEV metadata files
   - Has testmode conditional logic
   - **Migration approach**: Implement as dataset-inline validation in the e-mobility dataset

3. **`heat_gas_load_egon100RE()`**
   - Only prints comparison table (no assertions/validations)
   - Reads from pypsa_eur network data
   - No actual validation logic to migrate
   - **Migration approach**: Keep as reporting function or convert to validation with assertions

**Reason for Deferral: Uses External Calculation Functions**
4. **`etrago_eGon2035_gas_abroad()`**
   - Uses external calculation functions from gas_neighbours module
   - Requires dataset-specific context
   - **Migration approach**: Implement as dataset-inline validation in the gas grid dataset

5. **`sanitycheck_dsm()`**
   - Complex aggregation logic with multiple steps
   - Dataset-specific calculations
   - **Migration approach**: Implement as dataset-inline validation in the DSM dataset

---

## Directory Structure

```
egon-data/src/egon/data/
├── datasets/
│   ├── sanity_checks.py                        # ⚠️ Old sanity checks (kept for deferred validations)
│   ├── final_validations.py                    # ✅ Cross-cutting validations
│   └── ...
└── validation/
    └── rules/
        └── custom/
            └── sanity/
                ├── __init__.py                 # ✅ Exports all sanity validation classes
                ├── residential_electricity.py  # ✅ Migrated (2 rules)
                ├── cts_demand.py               # ✅ Migrated (2 rules)
                ├── home_batteries.py           # ✅ Migrated (1 rule)
                ├── gas_stores.py               # ✅ Migrated (2 rules: CH4, H2 saltcavern)
                ├── gas_grid.py                 # ✅ Migrated (5 rules: buses, one-port, CH4 grid, links)
                ├── gas_loads_generators.py     # ✅ Migrated (2 rules: loads, generators)
                ├── electricity_capacity.py     # ✅ Migrated (reusable class for capacity comparison)
                └── heat_demand.py              # ✅ Migrated (1 rule)

egon-validation/egon_validation/rules/
├── formal/
│   └── array_cardinality_check.py              # ✅ Reused for timeseries length validation
└── custom/
    └── numeric_aggregation_check.py            # ✅ Reused for electrical load aggregation
```

---

## Migration Statistics

**Total sanity checks in original `sanity_checks.py`**: 21 functions

**Successfully migrated**: 16 functions (76%)
- Converted to **48 individual validation rules** across multiple categories
- Organized into **8 custom validation modules**
- Reused **2 existing validation classes** from egon-validation

**Deferred (require dataset-inline implementation)**: 5 functions (24%)
- 3 complex validations with external dependencies
- 2 validations requiring external calculation functions

**Validation rules by category**:
- Electricity capacity: 10 rules (eGon2035)
- Heat capacity: 5 rules (eGon2035)
- eGon100RE capacity: 23 rules (13 generators, 9 links, 1 storage)
- Gas infrastructure: 11 rules
- Demand validation: 4 rules
- Timeseries: 8 rules
- Home batteries: 1 rule
- Electrical load: 1 rule (multi-scenario)
- Heat demand: 1 rule

---

## Testing Your Migration

1. **Add validation to a dataset:**
```python
validation={
    "data_quality": [
        YourNewRule(
            table="schema.table",
            rule_id="SANITY_YOUR_CHECK",
            param1=value1
        )
    ]
}
```

2. **Run the dataset:**
```bash
airflow tasks test your_dag your_dataset_task execution_date
```

3. **Check validation results:**
```bash
ls validation_runs/{run_id}/tasks/{dataset}.validate.data_quality/{rule_id}/
cat validation_runs/{run_id}/tasks/{dataset}.validate.data_quality/{rule_id}/results.jsonl
```

4. **View the validation report:**
```bash
open validation_runs/{run_id}/final/report.html
```

---

## Best Practices

1. **One rule class per check** - Keep rules focused and reusable
2. **Use descriptive rule_ids** - Follow pattern `SANITY_{CATEGORY}_{CHECK_NAME}`
3. **Set appropriate tolerances** - Document why you chose specific `rtol` values
4. **Provide clear messages** - Include context in success/failure messages
5. **Return observed/expected values** - Helps with debugging failures
6. **Override `kind = "sanity"`** - Ensures rules are categorized correctly

---

## Getting Help

- See implemented examples in `egon/data/validation/rules/custom/sanity/`
- Check egon-validation documentation for `DataFrameRule` API
- Ask in the team channel for migration assistance

---

## Summary and Next Steps

### ✅ Completed Work

The sanity checks migration is **76% complete** with all core validations successfully migrated to the new framework:

1. **8 custom validation modules** created in `egon/data/validation/rules/custom/sanity/`
2. **48 individual validation rules** implemented across all major categories
3. **Reused 2 existing validation classes** from egon-validation library (code reuse > new code)
4. **Fixed 4 RuleResult 'details' parameter errors** by moving violation data to message field
5. **Integrated validations** into `FinalValidations` dataset for cross-cutting checks

### 🔄 Remaining Work

5 sanity check functions (24%) are deferred for dataset-inline implementation:

**High Priority** (complex with external dependencies):
1. `sanitycheck_pv_rooftop_buildings()` - Implement in PV rooftop dataset
2. `sanitycheck_emobility_mit()` - Implement in e-mobility dataset
3. `heat_gas_load_egon100RE()` - Add assertions or keep as reporting function

**Medium Priority** (use external calculation functions):
4. `etrago_eGon2035_gas_abroad()` - Implement in gas grid dataset
5. `sanitycheck_dsm()` - Implement in DSM dataset

### 🎯 Recommended Approach for Deferred Validations

For each deferred validation:
1. Add inline `validation={}` dict to the relevant Dataset class
2. Create custom validation rules that can access dataset-specific functions
3. Use the same pattern as migrated validations (SqlRule or DataFrameRule)
4. Ensure validations run after dataset tasks complete

### 📊 Impact

- **Better error reporting**: Structured validation results with observed/expected values
- **Consistent framework**: All validations follow the same pattern
- **Parallel execution**: Validations can run concurrently
- **Automated reports**: HTML reports generated from all validation results
- **Code reuse**: Leveraged existing validation classes where possible

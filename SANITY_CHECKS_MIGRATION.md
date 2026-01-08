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

### ⚠️ CTS Demand (Migrated But Not Yet Integrated)
- `cts_electricity_demand_share()` → `CtsElectricityDemandShare` - **Rule exists but not yet integrated into CtsElectricityDemand dataset**
- `cts_heat_demand_share()` → `CtsHeatDemandShare` - **Rule exists but not yet integrated into CTS heat demand dataset**

**Status:** Rules are fully implemented and tested but need to be added to their respective datasets using the inline validation pattern. See "Using Inline Validations in Datasets" (line 110) for integration examples.

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
  - Validates ALL 24 array columns across 5 component types (generator, load, link, store, storage)
  - **Generator timeseries (5):** p_set, q_set, p_min_pu, p_max_pu, marginal_cost
  - **Load timeseries (2):** p_set, q_set
  - **Link timeseries (5):** p_set, p_min_pu, p_max_pu, efficiency, marginal_cost
  - **Storage timeseries (7):** p_set, q_set, p_min_pu, p_max_pu, state_of_charge_set, inflow, marginal_cost
  - **Store timeseries (5):** p_set, q_set, e_min_pu, e_max_pu, marginal_cost
  - Leverages existing formal validation rule from egon-validation library
  - **Updated:** Now matches original dynamic column discovery behavior (sanity_checks.py:2465-2494)

### ✅ eGon100RE Capacity Validations
- `generators_links_storages_stores_100RE()` → `ElectricityCapacityComparison` (reused for eGon100RE!)
  - **Generators (13):** wind_onshore, wind_offshore, solar, solar_rooftop, run_of_river, oil, lignite, coal, solar_thermal_collector, geo_thermal, rural_solar_thermal, urban_central_gas_CHP, urban_central_solid_biomass_CHP
  - **Links (9):** central_gas_boiler, central_heat_pump, central_resistive_heater, OCGT, rural_biomass_boiler, rural_gas_boiler, rural_heat_pump, rural_oil_boiler, rural_resistive_heater
  - **Storage (1):** pumped_hydro
  - **Note:** Stores validation deferred (original function only prints, no validation logic)

### ✅ Electrical Load Demand
- `electrical_load_100RE()` → `ElectricalLoadAggregationValidation` + `ElectricalLoadSectorBreakdown`
  - **Total load validation:** `ElectricalLoadAggregationValidation` validates annual load sum (TWh) for all scenarios
    - Also checks max/min load (GW) - more comprehensive than original
    - Leverages existing custom validation rule from egon-validation library
  - **Sector breakdown validation:** `ElectricalLoadSectorBreakdown` validates eGon100RE by sector (new class!)
    - Residential: 90.4 TWh expected (from household_curves table)
    - Commercial: 146.7 TWh expected (from cts_curves table)
    - Industrial: 382.9 TWh expected (from osm_curves + sites_curves tables)
    - Total: 620.0 TWh expected (from etrago AC loads)
    - Validates each sector independently with 1% tolerance
    - Queries source tables directly matching original implementation
    - **Updated:** Now provides full sector granularity as in original (sanity_checks.py:2676-2784)

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
- CTS demand (electricity and heat shares) - **rules migrated but not yet integrated**
- Home batteries aggregation
- Gas infrastructure (stores, buses, grid, links, loads, generators)
- Electricity capacity (eGon2035 and eGon100RE generators, storage)
- Heat capacity (heat pumps, resistive heaters, solar thermal, geothermal)
- Timeseries length validation
- Electrical load aggregation
- Heat demand validation

### Integration Status

This section tracks where each migrated validation rule has been integrated into the pipeline.

**✅ Fully Integrated in FinalValidations (Cross-Cutting Validations):**
- Gas stores: 4 rule instantiations (CH4 × 2 scenarios, H2 saltcavern × 2 scenarios)
- Gas grid buses: 5 rule instantiations (isolated buses + bus counts for CH4 and H2_grid)
- Gas one-port connections: 10 rule instantiations (loads, generators, stores validation)
- Gas links connections: 11 rule instantiations (CH4, H2_feedin, conversions, power coupling, etc.)
- Gas loads and generators capacity: 3 rule instantiations (CH4/H2 industry loads, CH4 generators)
- Electricity capacity: 33 rule instantiations (10 for eGon2035 + 23 for eGon100RE)
- Heat capacity: 5 rule instantiations (heat pumps, resistive heaters, solar/geo thermal)
- Timeseries length: 24 rule instantiations (all array columns across 5 component types)
- Electrical load: 2 rule instantiations (total aggregation + sector breakdown)
- Heat demand: 1 rule instantiation (eGon2035 validation)
- **Total in FinalValidations: 103 rule instantiations**

**✅ Fully Integrated as Dataset-Inline Validations:**
- ResidentialElectricityDemand dataset: 2 rules (annual sum + household refinement)
  - Location: `src/egon/data/datasets/electricity_demand/__init__.py` (lines 60-73)
- Storages dataset: 2 rule instantiations (home batteries aggregation × 2 scenarios)
  - Location: `src/egon/data/datasets/storages/__init__.py` (lines 103-118)
- **Total dataset-inline: 4 rule instantiations**

**⚠️ Migrated But Not Yet Integrated:**
- `CtsElectricityDemandShare`: Rule class exists in `rules/custom/sanity/cts_demand.py`
  - **Action needed**: Add to CtsElectricityDemand dataset validation dict
  - **Pattern**: See ResidentialElectricityDemand integration example (line 60-73)
- `CtsHeatDemandShare`: Rule class exists in `rules/custom/sanity/cts_demand.py`
  - **Action needed**: Add to CTS heat demand dataset validation dict
  - **Pattern**: See example in migration doc line 110-150

**Status Summary:**
- ✅ 107 validation rule instantiations actively running in pipeline
- ⚠️ 2 validation rule classes awaiting dataset integration
- 📊 Total: 17 reusable validation rule classes created

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
│   ├── sanity_checks.py                        # ⚠️ Old sanity checks (STILL IN USE - see note below)
│   ├── final_validations.py                    # ✅ Cross-cutting validations (103 rule instantiations)
│   ├── electricity_demand/__init__.py          # ✅ Uses residential electricity validations (2 rules)
│   ├── storages/__init__.py                    # ✅ Uses home batteries validation (2 instantiations)
│   └── ...
└── validation/
    └── rules/
        └── custom/
            └── sanity/
                ├── __init__.py                 # ✅ Exports all sanity validation classes (17 total)
                ├── residential_electricity.py  # ✅ Migrated & integrated (2 rule classes)
                ├── cts_demand.py               # ⚠️ Migrated but not integrated (2 rule classes)
                ├── home_batteries.py           # ✅ Migrated & integrated (1 rule class)
                ├── gas_stores.py               # ✅ Migrated & integrated (2 rule classes)
                ├── gas_grid.py                 # ✅ Migrated & integrated (5 rule classes)
                ├── gas_loads_generators.py     # ✅ Migrated & integrated (2 rule classes)
                ├── electricity_capacity.py     # ✅ Migrated & integrated (1 reusable rule class)
                ├── electrical_load_sectors.py  # ✅ Migrated & integrated (1 rule class)
                └── heat_demand.py              # ✅ Migrated & integrated (1 rule class)

egon-validation/egon_validation/rules/
├── formal/
│   └── array_cardinality_check.py              # ✅ Reused for timeseries length validation
└── custom/
    └── numeric_aggregation_check.py            # ✅ Reused for electrical load aggregation
```

**Note on old sanity_checks.py:**
- The old `sanity_checks.py` file is **still being used** by the `SanityChecks` dataset in the pipeline
- Location: `src/egon/data/airflow/dags/pipeline.py` (line 765)
- It currently runs the 5 deferred validation functions that haven't been migrated yet
- Once remaining functions are migrated, this file can be deprecated
- Migrated functions in this file are no longer called by the SanityChecks dataset

---

## Migration Statistics

**Total sanity checks in original `sanity_checks.py`**: 21 functions

**Successfully migrated**: 16 functions (76%)
- Created **17 reusable validation rule classes** (not counting reused egon-validation classes)
- Deployed as **110+ rule instantiations** across the pipeline
  - Each rule class can be instantiated multiple times for different scenarios/carriers
  - Example: `ElectricityCapacityComparison` is instantiated 33 times for different carriers/scenarios
- Organized into **9 custom validation modules**
- Reused **2 existing validation classes** from egon-validation library

**Deferred (require dataset-inline implementation)**: 5 functions (24%)
- 3 complex validations with external dependencies
- 2 validations requiring external calculation functions

**Rule instantiation breakdown**:
- **FinalValidations (cross-cutting)**: 103 instantiations
  - Gas stores: 4 (CH4 × 2 scenarios, H2 saltcavern × 2 scenarios)
  - Gas grid buses: 5 (isolated + count checks)
  - Gas one-port: 10 (load/generator/store connection validation)
  - Gas links: 11 (various carrier types)
  - Gas loads/generators: 3 (industry loads, generators)
  - Electricity capacity: 33 (10 eGon2035 + 23 eGon100RE)
  - Heat capacity: 5 (eGon2035)
  - Timeseries length: 24 (all array columns × 5 component types)
  - Electrical load: 2 (total aggregation + sector breakdown)
  - Heat demand: 1 (eGon2035)

- **Dataset-inline validations**: 4 instantiations
  - ResidentialElectricityDemand: 2 (annual sum + household refinement)
  - Storages: 2 (home batteries × 2 scenarios)

- **Migrated but not integrated**: 2 rule classes
  - CtsElectricityDemandShare (awaiting integration into CtsElectricityDemand dataset)
  - CtsHeatDemandShare (awaiting integration into CTS heat demand dataset)

**Total active validations**: 107 rule instantiations running in pipeline

**Recent Updates**:
- **2025-12-30**: Timeseries validation coverage expanded (8 → 24 array columns); Electrical load sector breakdown implemented
- **2026-01-07**: Documentation updated to clarify rule classes vs instantiations; Integration status tracking added

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

1. **17 reusable validation rule classes** created in `egon/data/validation/rules/custom/sanity/`
2. **110+ rule instantiations** deployed across the pipeline
   - 103 instantiations in FinalValidations (cross-cutting validations)
   - 4 instantiations in dataset-inline validations (ResidentialElectricityDemand, Storages)
   - 2 rule classes migrated but awaiting integration (CTS demand)
3. **9 custom validation modules** organized by domain (gas, electricity, heat, demand, etc.)
4. **Reused 2 existing validation classes** from egon-validation library (code reuse > new code)
5. **Full timeseries coverage** - All 24 array columns validated (matches original dynamic discovery)
6. **Sector breakdown validation** - Electrical load validated by sector (residential, commercial, industrial)
7. **107 validations actively running** in the pipeline with structured reporting

### 🔄 Remaining Work

**5 sanity check functions (24%)** are deferred for dataset-inline implementation:

**High Priority** (complex with external dependencies):
1. `sanitycheck_pv_rooftop_buildings()` - Implement in PV rooftop dataset
2. `sanitycheck_emobility_mit()` - Implement in e-mobility dataset
3. `heat_gas_load_egon100RE()` - Add assertions or keep as reporting function

**Medium Priority** (use external calculation functions):
4. `etrago_eGon2035_gas_abroad()` - Implement in gas grid dataset
5. `sanitycheck_dsm()` - Implement in DSM dataset

**Low Priority** (integration pending):
6. Integrate `CtsElectricityDemandShare` into CtsElectricityDemand dataset
7. Integrate `CtsHeatDemandShare` into CTS heat demand dataset

### 🎯 Recommended Approach for Remaining Work

**For CTS Validation Integration (Quick Win):**
The CTS validation rules already exist and just need to be integrated:
1. Open `src/egon/data/datasets/electricity_demand/__init__.py`
2. Import the CTS rules at the top:
   ```python
   from egon.data.validation.rules.custom.sanity import (
       CtsElectricityDemandShare,
       CtsHeatDemandShare,
   )
   ```
3. Add validation dict to `CtsElectricityDemand` class (see ResidentialElectricityDemand example, lines 60-73)
4. Test and verify

**For Deferred Validations (Complex):**
For the 5 remaining sanity check functions:
1. Add inline `validation={}` dict to the relevant Dataset class
2. Create custom validation rules that can access dataset-specific functions
3. Use the same pattern as migrated validations (SqlRule or DataFrameRule)
4. Ensure validations run after dataset tasks complete
5. For complex checks with visualizations (PV rooftop), consider splitting:
   - Validation logic → DataFrameRule (automated)
   - Visualization logic → Separate reporting function (manual)

### 📊 Impact

- **Better error reporting**: Structured validation results with observed/expected values
- **Consistent framework**: All validations follow the same pattern
- **Parallel execution**: Validations can run concurrently
- **Automated reports**: HTML reports generated from all validation results
- **Code reuse**: Leveraged existing validation classes where possible

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

## Completed Migrations

All sanity checks have been successfully migrated to validation rules:

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
- `etrago_eGon2035_gas_DE()` → `GasLoadsCapacity` + `GasGeneratorsCapacity`

### ✅ Electricity Capacity
- `etrago_eGon2035_electricity()` → `ElectricityCapacityComparison`
  - Validates: wind_onshore, wind_offshore, solar, solar_rooftop, biomass, run_of_river, reservoir, oil, others, pumped_hydro

### ✅ Heat Supply Capacity
- `etrago_eGon2035_heat()` → `ElectricityCapacityComparison` (reused for heat)
  - Links: central_heat_pump, rural_heat_pump, central_resistive_heater
  - Generators: solar_thermal_collector, geo_thermal

### ✅ Timeseries Length
- `etrago_timeseries_length()` → `ArrayCardinalityValidation` (reused from egon-validation)
  - Validates ALL 24 array columns across 5 component types

### ✅ eGon100RE Capacity Validations
- `generators_links_storages_stores_100RE()` → `ElectricityCapacityComparison`

### ✅ Electrical Load Demand
- `electrical_load_100RE()` → `ElectricalLoadAggregationValidation` + `ElectricalLoadSectorBreakdown`

### ✅ Heat Demand
- Heat demand validation → `HeatDemandValidation`

### ✅ DSM (Demand Side Management)
- `sanitycheck_dsm()` → `DSMTimeseries`
  - Validates link timeseries (p_min_pu, p_max_pu * p_nom) match individual DSM data
  - Validates store timeseries (e_min_pu, e_max_pu * e_nom) match individual DSM data
  - Uses np.allclose with atol=1e-01 for power comparisons

### ✅ PV Rooftop Buildings
- `sanitycheck_pv_rooftop_buildings()` → `PvRooftopBuildingsValidation`
  - Checks all PV installations have valid building assignments
  - Compares capacity against scenario_data (eGon2035) or scenario_capacities (eGon100RE)
  - Includes boundary adjustment for Schleswig-Holstein

### ✅ E-Mobility (Motorized Individual Travel)
- `sanitycheck_emobility_mit()` → 10 rule classes:
  - `EVAllocationCount` - EV counts match scenario target (only if TESTMODE_OFF)
  - `EVGridDistrictAllocation` - EVs allocated to grid districts match target
  - `EVTripTimeranges` - Trips have valid timesteps (0 to 35040)
  - `EVTripChargingDemand` - Charging demand can be covered by available power
  - `EVModelComponentsCreated` - Grid districts with EVs have model components
  - `EVModelTimeseriesLength` - All timeseries have 8760 steps
  - `EVModelEnergyDemand` - Total energy within 10% of approximation
  - `EVModelStorageCapacity` - Storage capacity within 1% of simBEV data
  - `EVModelSoCConstraint` - e_min_pu < e_max_pu for all timesteps
  - `EVLowflexDrivingLoad` - Driving load matches theoretical (charging * eta_cp)

### ✅ Gas Abroad
- `etrago_eGon2035_gas_abroad()` → 6 rule classes:
  - `GasBusesIsolatedAbroad` - No isolated CH4 buses abroad
  - `CH4LoadsAbroad` - CH4 loads abroad vs TYNDP data
  - `H2LoadsAbroad` - H2_for_industry loads abroad vs reference
  - `CH4GeneratorsAbroad` - CH4 generators abroad vs TYNDP
  - `CH4StoresAbroad` - CH4 stores abroad vs SciGRID_gas
  - `CH4GridLinksAbroad` - Crossborder CH4 grid capacity vs reference
  - Note: Original only logged deviations; rules add pass/fail with 10% tolerance

### ✅ Heat/Gas Load PyPSA-Eur Comparison
- `heat_gas_load_egon100RE()` → `HeatGasLoadPypsaEurComparison`
  - Compares etrago loads (by carrier) against PyPSA-Eur network data
  - Note: Original only printed comparison table; rule adds pass/fail with 10% tolerance

---

## Directory Structure

```
egon-data/src/egon/data/
├── datasets/
│   ├── sanity_checks.py                        # ⚠️ Old sanity checks (can be deprecated)
│   ├── final_validations.py                    # ✅ Cross-cutting validations
│   ├── electricity_demand/__init__.py          # ✅ Uses residential electricity validations
│   ├── storages/__init__.py                    # ✅ Uses home batteries validation
│   └── ...
└── validation/
    └── rules/
        └── custom/
            └── sanity/
                ├── __init__.py                 # ✅ Exports all sanity validation classes
                ├── residential_electricity.py  # ✅ 2 rule classes
                ├── cts_demand.py               # ✅ 2 rule classes
                ├── home_batteries.py           # ✅ 1 rule class
                ├── gas_stores.py               # ✅ 2 rule classes
                ├── gas_grid.py                 # ✅ 5 rule classes
                ├── gas_loads_generators.py     # ✅ 2 rule classes
                ├── electricity_capacity.py     # ✅ 1 rule class
                ├── electrical_load_sectors.py  # ✅ 1 rule class
                ├── heat_demand.py              # ✅ 1 rule class
                ├── dsm.py                      # ✅ 1 rule class
                ├── pv_rooftop.py               # ✅ 1 rule class
                ├── emobility_mit.py            # ✅ 10 rule classes
                ├── gas_abroad.py               # ✅ 6 rule classes
                └── heat_gas_load.py            # ✅ 1 rule class

egon-validation/egon_validation/rules/
├── formal/
│   └── array_cardinality_check.py              # ✅ Reused for timeseries length validation
└── custom/
    └── numeric_aggregation_check.py            # ✅ Reused for electrical load aggregation
```

---

## Migration Statistics

**Total sanity checks in original `sanity_checks.py`**: 21 functions

**Successfully migrated**: 21 functions (100%)

- Created **36 reusable validation rule classes**
- Organized into **14 custom validation modules**
- Reused **2 existing validation classes** from egon-validation library

**Rule class breakdown by module**:
- `residential_electricity.py`: 2 classes
- `cts_demand.py`: 2 classes
- `home_batteries.py`: 1 class
- `gas_stores.py`: 2 classes
- `gas_grid.py`: 5 classes
- `gas_loads_generators.py`: 2 classes
- `electricity_capacity.py`: 1 class
- `electrical_load_sectors.py`: 1 class
- `heat_demand.py`: 1 class
- `dsm.py`: 1 class
- `pv_rooftop.py`: 1 class
- `emobility_mit.py`: 10 classes
- `gas_abroad.py`: 6 classes
- `heat_gas_load.py`: 1 class

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

For validations tied to a specific dataset, add them inline to that dataset:

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

For validations that check data consistency **across multiple datasets**, add them to the `FinalValidations` dataset:

```python
# In: src/egon/data/datasets/final_validations.py

from egon.data.validation.rules.custom.sanity import (
    CH4StoresCapacity,
    H2SaltcavernStoresCapacity,
    DSMTimeseries,
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
                ],
                "dsm": [
                    DSMTimeseries(...),
                ],
                # Add new category if needed
            },
        )
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

## Summary

### ✅ Migration Complete

All sanity checks have been successfully migrated to the new validation framework:

1. **36 reusable validation rule classes** created in `egon/data/validation/rules/custom/sanity/`
2. **14 custom validation modules** organized by domain (gas, electricity, heat, demand, emobility, etc.)
3. **Reused 2 existing validation classes** from egon-validation library
4. **Full coverage** of all original sanity check functions

### 📊 Impact

- **Better error reporting**: Structured validation results with observed/expected values
- **Consistent framework**: All validations follow the same pattern
- **Parallel execution**: Validations can run concurrently
- **Automated reports**: HTML reports generated from all validation results
- **Code reuse**: Leveraged existing validation classes where possible

### Note on Original sanity_checks.py

The original `sanity_checks.py` file can now be deprecated as all functions have been migrated to validation rules. The migrated rules provide the same validation logic with improved reporting and integration into the validation framework.
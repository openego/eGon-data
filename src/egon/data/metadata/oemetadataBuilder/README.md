# oemetadataBuilder

This module implements functionality to create new oemetadata documents as part of other processing steps. This design is useful to integrate oemetadata creation into a pipeline like egon-data.

Note: Code will be moved to the `omi` package once python version is updates in egon-data and initial developments are done. This makes a lot of sense as the Builder implements a generic way of creating metadata and the use case is relevant in general. One great and highly relevant contribution from Regon to the OpenEnergyFamily.

## Design

The main concept we implement is metadata for datasets which can be managed and created in a modular and developer friendly way. The datasets definition meant here is more general then the egon-data dataset usage as multiple data resources can be part of that dataset. A egon-data dataset will implement the resource builder which can generate oemetadata for that single resource. Some of this metadata derived and automatically generated then some information which applies to more then one resource is added using templates and dataset-individual metadata can added using overlays where YAML files are used to specify metadata elements. The builder can apply all this layers of metadata information and "stack" them together into n resource metadata objects - as described one for each dataset. This part is inspired by the current metadata implementation where metadata is generated as part of ech the dataset task in the airflow pipeline.

The second feature the builder implements is the dataset composition. It is named "package" as naming it dataset could cause some confusion. It is a class which manages all resources and applies them into the oemetadata v2 structure. It will be used in a new task in the pipeline which generates a single metadata document.

Mixins are used as a common modularization pattern to keep the builder classes cleaner.

## Usage

In any dataset use:

```python
def add_metadata_vg250_zensus():
    engine: Engine = db.engine()
    schema, table = "boundaries", "egon_map_zensus_vg250"
    yaml_path = "metadata/overlay/zensus/egon_map_zensus_vg250.yaml"

    (OEMetadataBuilder()
        .from_template()
        .apply_yaml(yaml_path)
        .set_basic(name=f"{schema}.{table}")
        .auto_resource_from_table(
            engine, schema, table,
            geom_cols=["zensus_geom"],
        )
        .apply_field_hints_from_yaml()
        .finalize(license_check=False)
        .save_as_table_comment(engine, schema, table)
    )
```

OPTIONAL: in airflow pipeline we could use a helper like this one when strong metadata

```python
# airflow callable
def write_oemetadata(engine: Engine, schema: str, table: str, yaml_path: str):
    (OEMetadataBuilder()
        .from_template()
        .apply_yaml(yaml_path)
        .set_basic(name=f"{schema}.{table}")
        .auto_resource_from_table(engine, schema, table)
        .apply_field_hints_from_yaml()
        .finalize(license_check=False)
        .save_as_table_comment(engine, schema, table)
    )
```

Then to build the package like the code below (not all implemented yet):

```python
from egon.data.metadata.registry import iter_resources

pack = OEMetadataPackage().set_root(
    name="egon-datapackage-2025-08-13",
    title="eGon data release",
    description="All resources for this run",
    id_="https://example.org/egon/datapackage/2025-08-13"
)

for res in iter_resources():
    pack.add_resource(res)

pack.finalize()
json_text = pack.as_json()

```

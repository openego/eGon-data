from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import datetime as dt
import json

from geoalchemy2 import Geometry
from omi.base import MetadataSpecification, get_metadata_specification
from omi.validation import validate_metadata  # parse_metadata
from sqlalchemy import MetaData, Table, inspect
from sqlalchemy.dialects.postgresql.base import ischema_names
from sqlalchemy.engine import Engine
import yaml  # PyYAML

# ---- Optional: your project settings/hooks
# from egon.data import db, logger
from egon.data.metadata import settings

# Geometry awareness for reflection
ischema_names["geometry"] = Geometry  # generic
# You can add specific geometry columns later per-table via kwargs


def _today() -> str:
    return dt.date.today().isoformat()


def _deep_merge(base: dict, override: dict) -> dict:
    """
    Deep merge with 'override wins', recursively.
    Lists are replaced (not merged) by default to avoid subtle duplication.
    """
    out = deepcopy(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = deepcopy(v)
    return out


def _sqlatype_to_oem_type(sa_type: str) -> str:
    """
    Map SQLAlchemy reflected type string -> OEM v2 field.type
    Keep it simple and deterministic; adjust as needed.
    """
    t = sa_type.lower()
    # geometry
    if "geometry" in t:
        return "geometry"
    # integers
    if any(x in t for x in ["int", "serial", "bigint", "smallint"]):
        return "integer"
    # floats / numeric
    if any(x in t for x in ["float", "double", "numeric", "real", "decimal"]):
        return "number"
    # booleans
    if "bool" in t:
        return "boolean"
    # timestamp/date/time
    if "timestamp" in t or "timestamptz" in t:
        return "datetime"
    if t == "date":
        return "date"
    if t == "time":
        return "time"
    # text-ish
    if any(
        x in t for x in ["text", "char", "string", "uuid", "json", "jsonb"]
    ):
        return "string"
    # fallback
    return "string"


@dataclass
class ResourceField:
    """
    Minimal implementation of oemetadata v2 resource structure.
    Eases usage in Python.
    """

    name: str
    description: Optional[str] = None
    type: str = "string"
    unit: Optional[str] = None
    nullable: Optional[bool] = None

    def to_dict(self) -> dict:
        d = {
            "name": self.name,
            "type": self.type,
        }
        # include optional keys only when provided
        if self.description is not None:
            d["description"] = self.description
        if self.unit is not None:
            d["unit"] = self.unit
        if self.nullable is not None:
            d["nullable"] = self.nullable
        return d


class OEMetadataResourceBuilder:
    """
    Single, reusable builder for OEP oemetadata v2 using omi as source of truth.

    Typical flow:
      builder = OEMetadataBuilder().from_template()
                                   .apply_yaml("dataset_meta.yaml")
                                   .auto_resource_from_table(engine, "schema", "table", geom_cols=["geom"])
                                   .set_basic(name="schema.table", title="...", description="...")
                                   .finalize()
      payload = builder.as_json()  # validated JSON string
      builder.save_as_table_comment(db_engine, "schema", "table")  # optional
    """  # noqa: E501

    def __init__(self, version: str = settings.OEMETADATA_VERSION) -> None:
        self.spec: MetadataSpecification = get_metadata_specification(version)
        self._meta: Dict[str, Any] = {}
        self._validated: bool = False

    # ---- Required steps

    def from_template(self) -> "OEMetadataResourceBuilder":
        """
        Start from omi's template plus selected bits from example
        (context/metaMetadata).
        Ensures keys exist (empty strings/structures as per spec).
        """
        tpl = deepcopy(self.spec.template) if self.spec.template else {}
        if self.spec.example:
            # Copy @context + metaMetadata if present in example
            if "@context" in self.spec.example:
                tpl["@context"] = deepcopy(self.spec.example["@context"])
            if "metaMetadata" in self.spec.example:
                tpl["metaMetadata"] = deepcopy(
                    self.spec.example["metaMetadata"]
                )
        self._meta = tpl["resources"][0]
        self._validated = False
        return self

    def apply_yaml(
        self, yaml_path: str | None = None, yaml_text: str | None = None
    ) -> "OEMetadataResourceBuilder":
        """
        Merge user-provided YAML overrides into the current metadata object.
        You can allow either a file path or a YAML string (for testing).
        """
        if yaml_path:
            with open(yaml_path, "r", encoding="utf-8") as fh:
                override = yaml.safe_load(fh) or {}
        elif yaml_text:
            override = yaml.safe_load(yaml_text) or {}
        else:
            override = {}

        self._meta = _deep_merge(self._meta, override)
        self._validated = False
        return self

    def set_basic(
        self,
        name: str,
        title: Optional[str] = None,
        description: Optional[str] = None,
        language: Optional[List[str]] = None,
        publication_date: Optional[str] = None,
        dataset_id: Optional[str] = None,
    ) -> "OEMetadataResourceBuilder":
        """
        Convenience setter for common top-level fields.
        """
        if publication_date is None:
            publication_date = _today()
        patch = {
            "name": name,
            "publicationDate": publication_date,
        }
        if title is not None:
            patch["title"] = title
        if description is not None:
            patch["description"] = description
        if language is not None:
            patch["language"] = language
        if dataset_id is not None:
            patch["id"] = dataset_id

        self._meta = _deep_merge(self._meta, patch)
        self._validated = False
        return self

    def set_context(self, context_obj: dict) -> "OEMetadataResourceBuilder":
        self._meta = _deep_merge(self._meta, {"context": context_obj})
        self._validated = False
        return self

    def set_spatial(
        self,
        extent: Optional[str] = None,
        resolution: Optional[str] = None,
        location: Optional[Any] = None,
    ) -> "OEMetadataResourceBuilder":
        patch = {"spatial": {}}
        if location is not None:
            patch["spatial"]["location"] = location
        if extent is not None:
            patch["spatial"]["extent"] = extent
        if resolution is not None:
            patch["spatial"]["resolution"] = resolution
        self._meta = _deep_merge(self._meta, patch)
        self._validated = False
        return self

    def set_temporal(
        self,
        reference_date: Optional[str] = None,
        timeseries: Optional[dict] = None,
    ) -> "OEMetadataResourceBuilder":
        patch = {"temporal": {}}
        if reference_date is not None:
            # NOTE: your older code used 'referenceDate' vs
            # 'reference_date' in places.
            # OEM v2 uses 'referenceDate' (camelCase). Keep consistent here:
            patch["temporal"]["referenceDate"] = reference_date
        if timeseries is not None:
            patch["temporal"]["timeseries"] = timeseries
        self._meta = _deep_merge(self._meta, patch)
        self._validated = False
        return self

    # ---- Sources, licenses, contributors

    def add_source(self, source: dict) -> "OEMetadataResourceBuilder":
        self._meta.setdefault("sources", [])
        self._meta["sources"].append(source)
        self._validated = False
        return self

    def add_license(self, lic: dict) -> "OEMetadataResourceBuilder":
        self._meta.setdefault("licenses", [])
        self._meta["licenses"].append(lic)
        self._validated = False
        return self

    def add_contributor(
        self, contributor: dict
    ) -> "OEMetadataResourceBuilder":
        self._meta.setdefault("contributors", [])
        self._meta["contributors"].append(contributor)
        self._validated = False
        return self

    # ---- Resources

    def auto_resource_from_table(
        self,
        engine: Engine,
        schema: str,
        table: str,
        *,
        resource_name: Optional[str] = None,
        format_: str = "PostgreSQL",
        encoding: str = "UTF-8",
        primary_key: Optional[List[str]] = None,
        foreign_keys: Optional[List[dict]] = None,
        geom_cols: Optional[List[str]] = None,
        dialect: Optional[dict] = None,
        overwrite_existing: bool = False,
    ) -> "OEMetadataResourceBuilder":
        """
        Introspect a DB table and create a single tabular data resource entry.

        - Maps SQLA types to OEM types
        - Marks 'nullable' where possible
        - Recognizes geometry columns (if given in geom_cols) as 'geometry'

        If overwrite_existing=False and a resource already exists with the same
        name, it will be left as-is (you could add a flag to update instead).
        """
        if geom_cols is None:
            geom_cols = ["geom", "geometry", "geom_point", "geom_polygon"]

        # reflect
        meta = MetaData()
        tbl = Table(table, meta, schema=schema, autoload_with=engine)

        fields: List[ResourceField] = []
        for col in tbl.columns:
            sa_t = str(col.type)
            # if explicitly geometry by name, treat as geometry
            col_type = (
                "geometry"
                if col.name in geom_cols
                else _sqlatype_to_oem_type(sa_t)
            )
            fields.append(
                ResourceField(
                    name=col.name,
                    description=None,
                    type=col_type,
                    unit=None,
                    nullable=col.nullable,
                )
            )

        if not resource_name:
            resource_name = f"{schema}.{table}"

        resource = {
            "name": resource_name,
            # TODO: @jh-RLI The OEP will set this,
            # consider if local usage is important
            "path": None,
            "type": "table",
            "format": format_,
            "encoding": encoding,
            "schema": {
                "fields": [f.to_dict() for f in fields],
                "primaryKey": primary_key
                or self._best_guess_pk(engine, schema, table),
                "foreignKeys": foreign_keys or [],
            },
            "dialect": dialect or {"delimiter": None, "decimalSeparator": "."},
        }

        # install resources array
        self._meta.setdefault("resources", [])
        if overwrite_existing:
            self._meta["resources"] = [
                r
                for r in self._meta["resources"]
                if r.get("name") != resource_name
            ]
        # only add if not present
        if not any(
            r.get("name") == resource_name for r in self._meta["resources"]
        ):
            self._meta["resources"].append(resource)

        self._validated = False
        return self

    def _best_guess_pk(
        self, engine: Engine, schema: str, table: str
    ) -> List[str]:
        """
        Try to read PK columns via SQLAlchemy inspector, fallback to
        ['id'] if found, else [].
        """
        insp = inspect(engine)
        pk = insp.get_pk_constraint(table, schema=schema)
        cols = pk.get("constrained_columns") if pk else None
        if cols:
            return cols
        # common fallback
        columns = [c["name"] for c in insp.get_columns(table, schema=schema)]
        return ["id"] if "id" in columns else []

    # ---- Finalize/validate/serialize

    def finalize(
        self, license_check: bool = False
    ) -> "OEMetadataResourceBuilder":
        """
        Make minimal guarantees & validate with omi.
        """
        # Fill sane defaults if missing
        # self._meta.setdefault("publicationDate", _today())
        self._meta.setdefault("language", ["en-EN"])

        # TODO: @jh-RLI might be expensive
        # parse + validate with omi
        # parse_metadata expects string; serialize & round-trip to normalize
        # text = json.dumps(self._meta, ensure_ascii=False)
        # parsed = parse_metadata(text)

        # You can toggle license checks if you are mid-migration:
        validate_metadata(self._meta, check_license=license_check)

        # Reassign parsed (it may normalize the structure)
        # self._meta = parsed
        self._validated = True
        return self

    def as_dict(self) -> dict:
        if not self._validated:
            self.finalize()
        return deepcopy(self._meta)

    def as_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False)

    # ---- Optional convenience: store as comment on a table

    def save_as_table_comment(
        self, engine: Engine, schema: str, table: str
    ) -> None:
        """
        Store metadata JSON as a COMMENT ON TABLE ... (PostgreSQL).
        """
        payload = self.as_json().replace(
            "'", "''"
        )  # escape single-quotes for SQL literal
        full = f"{schema}.{table}"
        sql = f"COMMENT ON TABLE {full} IS '{payload}';"
        with engine.begin() as conn:
            conn.exec_driver_sql(sql)

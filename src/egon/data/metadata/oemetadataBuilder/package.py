from copy import deepcopy
from typing import Any, Dict
import json

from omi.base import get_metadata_specification
from omi.validation import validate_metadata  # parse_metadata
from sqlalchemy.engine import Engine

from egon.data.metadata import settings


class OEMetadataPackage:
    def __init__(self, version: str = settings.OEMETADATA_VERSION) -> None:
        self.spec = get_metadata_specification(version)
        self._doc: Dict[str, Any] = {
            "@context": "https://raw.githubusercontent.com/OpenEnergyPlatform/oemetadata/production/oemetadata/latest/context.json",  # noqa: E501
            "name": "",
            "title": "",
            "description": "",
            "@id": "",
            "resources": [],
            "metaMetadata": {
                "metadataVersion": version,
                "metadataLicense": {"name": "CC0-1.0"},
            },
        }
        self._validated = False

    def set_root(
        self,
        *,
        name: str,
        title: str = "",
        description: str = "",
        id_: str = "",
    ) -> "OEMetadataPackage":
        self._doc["name"] = name
        self._doc["title"] = title
        self._doc["description"] = description
        self._doc["@id"] = id_
        self._validated = False
        return self

    def add_resource(
        self,
        resource: dict,
        *,
        dedupe_by: str = "name",
        overwrite: bool = True,
    ) -> "OEMetadataPackage":
        if dedupe_by and overwrite:
            self._doc["resources"] = [
                r
                for r in self._doc["resources"]
                if r.get(dedupe_by) != resource.get(dedupe_by)
            ]
        self._doc["resources"].append(deepcopy(resource))
        self._validated = False
        return self

    def add_from_full_document(
        self, full_doc: dict, *, take_root_if_empty: bool = False
    ) -> "OEMetadataPackage":
        # Optionally fill root if still empty
        if take_root_if_empty and not self._doc["name"]:
            for k in ("name", "title", "description", "@id"):
                if k in full_doc:
                    self._doc[k] = full_doc[k]
        for r in full_doc.get("resources", []):
            self.add_resource(r)
        return self

    def add_from_table_comment(
        self, engine: Engine, schema: str, table: str
    ) -> "OEMetadataPackage":
        sql = """
        SELECT obj_description((quote_ident(%s)||'.'||quote_ident(%s))::regclass, 'pg_class') AS comment
        """  # noqa: E501
        with engine.begin() as conn:
            comment = conn.exec_driver_sql(sql, (schema, table)).scalar()
        if not comment:
            return self
        try:
            full_doc = json.loads(comment)
        except Exception:
            return self
        # Optionally validate the doc before merging
        try:
            validate_metadata(full_doc, check_license=False)
        except Exception:
            pass
        return self.add_from_full_document(full_doc)

    def finalize(self, *, license_check: bool = True) -> "OEMetadataPackage":
        validate_metadata(self._doc, check_license=license_check)
        self._validated = True
        return self

    def as_dict(self) -> dict:
        if not self._validated:
            self.finalize()
        return deepcopy(self._doc)

    def as_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False)

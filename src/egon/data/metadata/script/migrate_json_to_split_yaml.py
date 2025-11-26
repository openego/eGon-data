#!/usr/bin/env python3
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Tuple
import argparse
import json

import yaml

# ---------- YAML helpers ----------


def _dump_yaml(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(obj, f, sort_keys=False, allow_unicode=True)


# ---------- list/dict helpers for template inference ----------

_LIST_TEMPLATE_KEYS = {"keywords", "topics", "languages"}
_DICT_TEMPLATE_KEYS = {"context"}
_LIST_EQUALITY_KEYS = {
    "licenses"
}  # consider as common only if identical across all


def _deep_intersection_dict(dicts: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not dicts:
        return {}
    keys_in_all = set(dicts[0].keys())
    for d in dicts[1:]:
        keys_in_all &= set(d.keys())

    out: Dict[str, Any] = {}
    for k in keys_in_all:
        vals = [d[k] for d in dicts]
        if not all(type(v) is type(vals[0]) for v in vals):  # noqa: E721
            continue
        v0 = vals[0]
        if isinstance(v0, dict):
            # type: ignore[arg-type]
            sub = _deep_intersection_dict([v for v in vals])
            if sub:
                out[k] = sub
        elif isinstance(v0, list):
            if all(v == v0 for v in vals[1:]):
                out[k] = deepcopy(v0)
        else:
            if all(v == v0 for v in vals[1:]):
                out[k] = v0
    return out


def _all_identical(values: List[Any]) -> Tuple[bool, Any]:
    if not values:
        return False, None
    v0 = values[0]
    return (all(v == v0 for v in values[1:])), v0


# ---------- schema fix ----------


def _ensure_nullable_in_fields(resource: Dict[str, Any]) -> None:
    schema = resource.get("schema")
    if not isinstance(schema, dict):
        return
    fields = schema.get("fields")
    if not isinstance(fields, list):
        return
    for fld in fields:
        if isinstance(fld, dict) and "nullable" not in fld:
            fld["nullable"] = False


# ---------- names ----------


def _slug(s: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in s)


def _resource_filename(name: str) -> str:
    return f"{_slug(name)}.resource.yaml"


# ---------- core migration ----------


def _strip_resource_by_template(
    res: Dict[str, Any], tmpl: Dict[str, Any]
) -> Dict[str, Any]:
    out = deepcopy(res)

    # strip list-keys if identical
    for k in list(_LIST_TEMPLATE_KEYS | _LIST_EQUALITY_KEYS):
        if k in tmpl and k in out and out[k] == tmpl[k]:
            out.pop(k, None)

    # strip dict-keys by matching deep subset of template keys
    for k in _DICT_TEMPLATE_KEYS:
        if (
            k not in tmpl
            or k not in out
            or not isinstance(out[k], dict)
            or not isinstance(tmpl[k], dict)
        ):
            continue
        pruned = {}
        for kk, vv in out[k].items():
            if kk in tmpl[k] and tmpl[k][kk] == vv:
                continue
            pruned[kk] = vv
        if pruned:
            out[k] = pruned
        else:
            out.pop(k, None)

    return out


def _compute_template(resources: List[Dict[str, Any]]) -> Dict[str, Any]:
    template: Dict[str, Any] = {}
    if not resources:
        return template

    # dict keys (deep intersection)
    for k in _DICT_TEMPLATE_KEYS:
        dicts = [r.get(k, {}) for r in resources if isinstance(r.get(k), dict)]
        if len(dicts) == len(resources) and dicts:
            inter = _deep_intersection_dict(dicts)
            if inter:
                template[k] = inter

    # list equality keys (identical lists across all)
    for k in _LIST_EQUALITY_KEYS:
        lists = [r.get(k) for r in resources]
        if all(isinstance(v, list) for v in lists):
            same, val = _all_identical(lists)
            if same and val:
                template[k] = deepcopy(val)

    # simple list keys that must be identical across resources
    for k in _LIST_TEMPLATE_KEYS:
        vals = [r.get(k) for r in resources]
        if all(isinstance(v, list) for v in vals):
            same, val = _all_identical(vals)
            if same and val:
                template[k] = deepcopy(val)

    return template


def migrate_monolithic(
    data: Dict[str, Any], out_dir: Path, dataset_id: str
) -> None:
    resources = data.get("resources", [])
    if not isinstance(resources, list):
        resources = []

    # Build dataset block (skip @context/metaMetadata/resources)
    dataset: Dict[str, Any] = {
        k: v
        for k, v in data.items()
        if k not in {"@context", "metaMetadata", "resources"}
    }
    version = dataset.get("version", "OEMetadata-2.0.4")

    # Fix fields
    for r in resources:
        if isinstance(r, dict):
            _ensure_nullable_in_fields(r)

    # Compute template & strip
    template = _compute_template(resources)
    stripped_resources = [
        _strip_resource_by_template(r, template) for r in resources
    ]

    # Write YAMLs
    ds_path = out_dir / "datasets" / f"{dataset_id}.dataset.yaml"
    _dump_yaml(ds_path, {"version": version, "dataset": dataset})

    if template:
        tp_path = out_dir / "datasets" / f"{dataset_id}.template.yaml"
        _dump_yaml(tp_path, template)

    res_dir = out_dir / "resources" / dataset_id
    for r in stripped_resources:
        name = str(r.get("name") or "resource")
        _dump_yaml(res_dir / _resource_filename(name), r)

    print(
        f"[OK] {dataset_id}: wrote dataset + {len(stripped_resources)}"
        "resources"
    )


def migrate_resource_only(
    data: Dict[str, Any], out_dir: Path, dataset_id: str
) -> None:
    """
    For JSONs that contain a single resource (no top-level 'resources').
    We just write one resource YAML. Dataset/template can be added later.
    """
    name = str(data.get("name") or "resource")
    _ensure_nullable_in_fields(data)
    res_dir = out_dir / "resources" / dataset_id
    _dump_yaml(res_dir / _resource_filename(name), data)
    print(f"[OK] {dataset_id}: wrote single resource '{name}'")


# ---------- dataset_id inference ----------


def pick_dataset_id(
    mode: str,
    data: Dict[str, Any],
    file_path: Path,
    fixed_id: str | None,
) -> str:
    if mode == "fixed":
        if not fixed_id:
            raise ValueError(
                "--dataset-id is required when --dataset-id-mode=fixed"
            )
        return fixed_id
    if mode == "name":
        n = data.get("name")
        if isinstance(n, str) and n.strip():
            return _slug(n)
        return _slug(file_path.stem)
    if mode == "filename":
        return _slug(file_path.stem)
    if mode == "parent":
        return _slug(file_path.parent.name)
    raise ValueError(f"Unknown dataset-id-mode: {mode}")


# ---------- CLI ----------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Migrate OEMetadata JSON → split YAML"
        "(handles many files)."
    )
    ap.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to a JSON file or a directory to scan recursively.",
    )
    ap.add_argument(
        "--out-dir",
        required=True,
        type=Path,
        help="Root of the split-YAML metadata tree to write.",
    )
    ap.add_argument(
        "--dataset-id-mode",
        choices=["name", "filename", "parent", "fixed"],
        default="name",
        help="How to determine dataset_id per input file (default: name).",
    )
    ap.add_argument(
        "--dataset-id",
        default=None,
        help="Required if --dataset-id-mode=fixed; ignored otherwise.",
    )
    args = ap.parse_args()

    inputs: List[Path]
    if args.input.is_dir():
        inputs = sorted(args.input.rglob("*.json"))
        if not inputs:
            print(f"[WARN] No JSON files found under {args.input}")
            return
    else:
        inputs = [args.input]

    out_dir = args.out_dir

    for j in inputs:
        try:
            data = json.loads(j.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[SKIP] {j}: cannot read/parse JSON ({e})")
            continue

        try:
            dsid = pick_dataset_id(
                args.dataset_id_mode, data, j, args.dataset_id
            )
        except Exception as e:
            print(f"[SKIP] {j}: {e}")
            continue

        if isinstance(data.get("resources"), list):
            migrate_monolithic(data, out_dir, dsid)
        else:
            # treat as a single resource JSON (best-effort)
            migrate_resource_only(data, out_dir, dsid)


if __name__ == "__main__":
    main()

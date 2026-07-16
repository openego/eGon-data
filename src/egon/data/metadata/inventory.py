"""Static inventory of the pipeline's declared sources and targets.

Enumerates every ``Dataset`` class that declares ``DatasetSources`` /
``DatasetTargets`` and attributes each class to its DAG task group --
without importing any dataset module (pure AST parsing, so no airflow or
database dependencies are needed). This makes the inventory usable in CI
and on developer machines without a running pipeline.

The union of all declared target tables is the authoritative list of
"tables that need metadata" (there is no separate registry); the declared
sources drive provenance stubs. The task-group attribution maps every
class to its OEMetadata dataset id via ``dataset_metadata/dataset_ids.yaml``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import ast
import re

import yaml

import egon.data.airflow
import egon.data.datasets

DATASETS_DIR = Path(egon.data.datasets.__file__).parent
PIPELINE_PY = Path(egon.data.airflow.__file__).parent / "dags" / "pipeline.py"
DATASET_IDS_YAML = (
    Path(__file__).parent / "dataset_metadata" / ("dataset_ids.yaml")
)

UNMAPPED = "UNMAPPED"


@dataclass
class ClassIO:
    """Declared IO of one Dataset class, attributed to a task group."""

    name: str
    module: str
    task_group: str = UNMAPPED
    source_tables: dict = field(default_factory=dict)
    source_files: dict = field(default_factory=dict)
    source_urls: dict = field(default_factory=dict)
    target_tables: dict = field(default_factory=dict)
    target_files: dict = field(default_factory=dict)


def _dict_of_str(node):
    """Return {key: value} for an ast.Dict of string constants, else {}."""
    out = {}
    if not isinstance(node, ast.Dict):
        return out
    for k, v in zip(node.keys, node.values):
        if isinstance(k, ast.Constant) and isinstance(v, ast.Constant):
            out[k.value] = v.value
    return out


def _kwargs_of_call(call, kwarg_names):
    """Extract the given dict-valued keyword arguments from a Call node."""
    out = {name: {} for name in kwarg_names}
    for kw in call.keywords:
        if kw.arg in kwarg_names:
            out[kw.arg] = _dict_of_str(kw.value)
    return out


def _is_call_to(node, names):
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in names
    )


def _scan_module(path: Path) -> list[ClassIO]:
    """Extract sources/targets declarations from one module (AST only)."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return []
    results = []
    for cls in ast.walk(tree):
        if not isinstance(cls, ast.ClassDef):
            continue
        io = None
        for stmt in cls.body:
            if not isinstance(stmt, ast.Assign):
                continue
            names = [t.id for t in stmt.targets if isinstance(t, ast.Name)]
            if "targets" in names and _is_call_to(
                stmt.value, {"DatasetTargets"}
            ):
                io = io or ClassIO(name=cls.name, module=str(path))
                parts = _kwargs_of_call(stmt.value, ("tables", "files"))
                io.target_tables = parts["tables"]
                io.target_files = parts["files"]
            if "sources" in names and _is_call_to(
                stmt.value, {"DatasetSources"}
            ):
                io = io or ClassIO(name=cls.name, module=str(path))
                parts = _kwargs_of_call(
                    stmt.value, ("tables", "files", "urls")
                )
                io.source_tables = parts["tables"]
                io.source_files = parts["files"]
                io.source_urls = parts["urls"]
        if io is not None:
            results.append(io)
    return results


def _parse_pipeline_task_groups():
    """Attribute imported class/factory names to DAG task groups.

    Parses ``pipeline.py``: records the import table (name -> module) and,
    for each ``TaskGroup(group_id=...)`` block, which imported names are
    instantiated inside it. Bare references (dependency lists) are ignored
    so a class is attributed to the group that constructs it, not to
    groups that merely depend on it.
    """
    lines = PIPELINE_PY.read_text(encoding="utf-8").splitlines(keepends=True)
    tree = ast.parse("".join(lines))

    imports = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                imports[alias.asname or alias.name] = node.module

    # TaskGroup blocks: group_id may sit on the line after `TaskGroup(`.
    groups = []  # (group_id, start_line)
    for i, line in enumerate(lines, start=1):
        if 'group_id="' in line and "TaskGroup" in "".join(
            lines[max(0, i - 2) : i]
        ):
            gid = line.split('group_id="', 1)[1].split('"', 1)[0]
            groups.append((gid, i))
    spans = []
    for idx, (gid, start) in enumerate(groups):
        end = groups[idx + 1][1] - 1 if idx + 1 < len(groups) else len(lines)
        spans.append((gid, start, end))

    # Imported name instantiated in a group: `Name(` or `Name.` (submodule
    # factory). Word-boundary anchored so `Storages(` does not match inside
    # `CH4Storages(`.
    name_to_group = {}
    for name in imports:
        pat = re.compile(r"(?<![\w.])" + re.escape(name) + r"\s*[.(]")
        for gid, start, end in spans:
            if pat.search("".join(lines[start - 1 : end])):
                name_to_group.setdefault(name, gid)
                break

    # Class reached via submodule call (e.g. hh_profiles.HouseholdDemands).
    call_pat = re.compile(r"\b(\w+)\.(\w+)\s*\(")
    class_name_to_group = {}
    for gid, start, end in spans:
        for _mod, cls in call_pat.findall("".join(lines[start - 1 : end])):
            class_name_to_group.setdefault(cls, gid)

    return imports, name_to_group, class_name_to_group


def _module_key(path: str) -> str:
    """Turn a file path into its dotted module path."""
    p = path.replace("\\", "/")
    p = p.split("src/", 1)[-1].removesuffix(".py")
    p = p.removesuffix("/__init__")
    return p.replace("/", ".")


def scan_declared_io() -> list[ClassIO]:
    """Enumerate all Dataset classes with declared sources/targets.

    Returns one :class:`ClassIO` per class, with ``task_group`` resolved
    via ``pipeline.py`` (``UNMAPPED`` when the class is not wired into the
    main DAG, e.g. classes used only in alternate DAGs).
    """
    classes: list[ClassIO] = []
    for path in sorted(DATASETS_DIR.rglob("*.py")):
        classes.extend(_scan_module(path))

    imports, name_to_group, class_name_to_group = _parse_pipeline_task_groups()
    module_to_group = {}
    for name, group in name_to_group.items():
        mod = imports.get(name)
        if mod:
            module_to_group.setdefault(mod, group)

    for c in classes:
        if c.name in name_to_group:
            c.task_group = name_to_group[c.name]
        elif c.name in class_name_to_group:
            c.task_group = class_name_to_group[c.name]
        else:
            c.task_group = module_to_group.get(_module_key(c.module), UNMAPPED)
    return classes


def load_dataset_ids() -> dict[str, str]:
    """Load the task-group -> OEMetadata-dataset-id mapping."""
    doc = yaml.safe_load(DATASET_IDS_YAML.read_text(encoding="utf-8"))
    return dict(doc.get("task_groups") or {})


def filter_scope(classes: list[ClassIO], scope: str) -> list[ClassIO]:
    """Filter classes by task group, dataset id, or Dataset class name."""
    ids = load_dataset_ids()
    groups_for_id = {v: k for k, v in ids.items()}
    if scope in ids:  # a task group name
        return [c for c in classes if c.task_group == scope]
    if scope in groups_for_id:  # an OEMetadata dataset id
        group = groups_for_id[scope]
        return [c for c in classes if c.task_group == group]
    return [c for c in classes if c.name == scope]  # a class name


def expected_tables(classes: list[ClassIO]) -> dict[str, list[ClassIO]]:
    """Union of declared target tables -> the classes producing each."""
    out: dict[str, list[ClassIO]] = {}
    for c in classes:
        for table in c.target_tables.values():
            out.setdefault(table, []).append(c)
    return out


def consumed_tables(classes: list[ClassIO]) -> dict[str, list[ClassIO]]:
    """Union of declared source tables -> the classes consuming each."""
    out: dict[str, list[ClassIO]] = {}
    for c in classes:
        for table in c.source_tables.values():
            out.setdefault(table, []).append(c)
    return out
